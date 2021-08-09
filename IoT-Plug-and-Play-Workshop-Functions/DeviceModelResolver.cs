using Microsoft.Azure.DigitalTwins.Parser;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace IoT_Plug_and_Play_Workshop_Functions
{
    public class DeviceModelResolver
    {
        private const string _modelRepoUrl_Public = "https://devicemodels.azure.com";
        private static string _modelRepoUrl_Private = string.Empty;
        private static string _githubToken = string.Empty;
        private static HttpClient _httpClient = new HttpClient();
        private static ILogger _logger = null;
        private static string _modelRepoUrl;

        public DeviceModelResolver(ILogger logger)
        {
            _logger = logger;
        }

        public DeviceModelResolver(string repositoryUri)
        {
            _modelRepoUrl_Private = repositoryUri;
        }

        public DeviceModelResolver(string repositoryUri, string githubToken, ILogger logger)
        {
            _modelRepoUrl_Private = repositoryUri;
            _githubToken = githubToken;
            _logger = logger;
        }
        private bool IsValidDtmi(string dtmi)
        {
            // Regex defined at https://github.com/Azure/digital-twin-model-identifier#validation-regular-expressions
            Regex rx = new Regex(@"^dtmi:[A-Za-z](?:[A-Za-z0-9_]*[A-Za-z0-9])?(?::[A-Za-z](?:[A-Za-z0-9_]*[A-Za-z0-9])?)*;[1-9][0-9]{0,8}$");
            return rx.IsMatch(dtmi);
        }

        public string DtmiToPath(string dtmi)
        {
            if (IsValidDtmi(dtmi))
            {
                return $"/{dtmi.ToLowerInvariant().Replace(":", "/").Replace(";", "-")}.json";
            }
            return string.Empty;
        }

        public async Task<string> GetModelContentAsync(string dtmiPath, string repositoryUrl)
        {
            var jsonModel = string.Empty;
            try
            {
                var fullPath = new Uri($"{repositoryUrl}{dtmiPath}");
                if (!repositoryUrl.Equals(_modelRepoUrl_Public) && !string.IsNullOrEmpty(_githubToken))
                {
                    _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", _githubToken);
                }
                //_logger.LogInformation($"GetModelContentAsync(1) Url {fullPath}");
                jsonModel = await _httpClient.GetStringAsync(fullPath);
            }
            catch (Exception e)
            {
                _logger.LogError($"Error GetModelContentAsync(): {e.Message}");
            }
            return jsonModel;
        }

        public async Task<string> GetModelContentAsync(string dtmiPath, bool bPublicRepo)
        {
            var jsonModel = string.Empty;
            try
            {
                var fullPath = new Uri($"{(bPublicRepo == true ? _modelRepoUrl_Public : _modelRepoUrl_Private)}{dtmiPath}");
                if (!string.IsNullOrEmpty(_githubToken))
                {
                    _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", _githubToken);
                }
                //_logger.LogInformation($"GetModelContentAsync(2) Url {fullPath}");
                jsonModel = await _httpClient.GetStringAsync(fullPath);
            }
            catch (Exception e)
            {
                _logger.LogError($"Error GetModelContentAsync(): {e.Message}");
            }
            return jsonModel;
        }

        private async Task<IEnumerable<string>> DtmiResolver(IReadOnlyCollection<Dtmi> dtmis)
        {
            List<string> resolvedModels = new List<string>();
            foreach (var dtmi in dtmis)
            {
                //_logger.LogInformation($"Resolving {dtmi.AbsoluteUri}");
                var dtmiPath = DtmiToPath(dtmi.AbsoluteUri);
                string dtmiContent = await GetModelContentAsync(dtmiPath, _modelRepoUrl);
                resolvedModels.Add(dtmiContent);
            }
            return await Task.FromResult(resolvedModels);
        }

        public async Task<IReadOnlyDictionary<Dtmi, DTEntityInfo>> ParseModelAsync(string dtmi)
        {
            string modelContent = string.Empty;
            IReadOnlyDictionary<Dtmi, DTEntityInfo> parseResult = null;
            List<string> listModelJson = new List<string>();

            string dtmiPath = DtmiToPath(dtmi);

            if (!string.IsNullOrEmpty(dtmiPath))
            {

                if (!string.IsNullOrEmpty(_modelRepoUrl_Private))
                {
                    modelContent = await GetModelContentAsync(dtmiPath, false);
                }

                if (string.IsNullOrEmpty(modelContent))
                {
                    // try public repo
                    modelContent = await GetModelContentAsync(dtmiPath, true);
                }

                if (!string.IsNullOrEmpty(modelContent))
                {
                    listModelJson.Add(modelContent);
                }

                try
                {
                    ModelParser dtdlModelParser = new ModelParser();
                    dtdlModelParser.DtmiResolver = DtmiResolver;

                    if (!string.IsNullOrEmpty(_modelRepoUrl_Private))
                    {
                        _modelRepoUrl = _modelRepoUrl_Private;
                    }
                    else
                    {
                        _modelRepoUrl = _modelRepoUrl_Public;
                    }
                    parseResult = await dtdlModelParser.ParseAsync(listModelJson);
                }
                catch (Exception e)
                {
                    _logger.LogError($"Error ParseModelAsync(): {e.Message}");
                }
            }
            return parseResult;
        }
    }
}

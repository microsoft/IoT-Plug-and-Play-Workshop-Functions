using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Azure.Core.Pipeline;
using System.Net.Http;
using Azure;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Net;

namespace IoT_Plug_and_Play_Workshop_Functions
{
    public static class Dps_Processor
    {
        private static readonly HttpClient _httpClient = new HttpClient();
        private static readonly WebClient _webClient = new WebClient();
        private static string _adtServiceUrl = Environment.GetEnvironmentVariable("ADT_HOST_URL");
        private static DigitalTwinsClient _adtClient = null;
        private static readonly string _modelRepoUrl = Environment.GetEnvironmentVariable("ModelRepository");
        private static readonly string _gitToken = Environment.GetEnvironmentVariable("GitToken");

        [FunctionName("Dps_Processor")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            log.LogInformation($"Request.Body: {requestBody}");
            string message = string.Empty;
            ResponseObj response = null;

            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string registrationId = data?.deviceRuntimeContext?.registrationId;
            string[] hubs = data?.linkedHubs.ToObject<string[]>();

            if (registrationId == null)
            {
                message = "Registration ID not provided";
            }
            else
            {
                dynamic payload = data?.deviceRuntimeContext?.payload;
                string modelId = payload.modelId;

                log.LogInformation($"RegID: {registrationId} ModelId: {modelId} Hub: {hubs[0]}");

                await ProcessADT(modelId, registrationId, log);

                response = new ResponseObj();

                response.iotHubHostName = hubs[0];
            }

            return (string.IsNullOrEmpty(message)) ? (ActionResult)new OkObjectResult(response) : new BadRequestObjectResult(message);
        }

        public static async Task<bool> ProcessADT(string dtmi, string regId, ILogger log)
        {

            if (string.IsNullOrEmpty(dtmi) || string.IsNullOrEmpty(regId))
            {
                return false;
            }
            else if (_adtClient == null && !string.IsNullOrEmpty(_adtServiceUrl))
            {
                try
                {
                    var credentials = new DefaultAzureCredential();
                    _adtClient = new DigitalTwinsClient(new Uri(_adtServiceUrl), credentials, new DigitalTwinsClientOptions { Transport = new HttpClientTransport(_httpClient) });
                    log.LogInformation("ADT service client connection created.");
                }
                catch (Exception e)
                {
                    log.LogError($"ADT service client connection failed. {e}");
                }
            }

            if (_adtClient != null)
            {
                bool bFoundTwin = false;
                bool bFoundModel = false;
                // check if twin exists for this device
                try
                {
                    //Response<DigitalTwinsModelData> modelData = await _adtClient.GetModelAsync(dtmi);
                    //if (modelData == null)
                    //{
                    //    log.LogError($"GetModelAsync returned empty model data for {dtmi}");
                    //    return false;
                    //}

                    string query = $"SELECT * FROM DigitalTwins T WHERE $dtId = '{regId}' AND IS_OF_MODEL('{dtmi}')";
                    AsyncPageable<BasicDigitalTwin> asyncPageableResponse = _adtClient.QueryAsync<BasicDigitalTwin>(query);

                    await foreach (BasicDigitalTwin twin in asyncPageableResponse)
                    {
                        // Get DT ID from the Twin
                        log.LogInformation($"Twin '{twin.Id}' with Registration ID '{regId}' found in DT");
                        bFoundTwin = true;
                    }
                }
                catch (RequestFailedException rex)
                {
                    log.LogError($"GetModelAsync: {rex.Status}:{rex.Message}");
                    return false;
                }

                if (bFoundTwin)
                {
                    log.LogInformation($"Twin found. ID: {regId} (Model: {dtmi})");
                    return true;
                }

                AsyncPageable<DigitalTwinsModelData> allModels = _adtClient.GetModelsAsync();
                await foreach (DigitalTwinsModelData model in allModels)
                {

                    if (model.Id.Equals(dtmi))
                    {
                        log.LogInformation($"Found model ID : {dtmi}");
                        bFoundModel = true;
                        break;
                    }
                }

                if (!bFoundModel)
                {
                    // create a model
                    // 1. Get model definition
                    string modelContent = string.Empty;
                    var modelList = new List<string>();
                    string dtmiPath = DtmiToPath(dtmi.ToString());


                    // if private repo is provided, resolve model with private repo first.
                    if (!string.IsNullOrEmpty(_modelRepoUrl))
                    {
                        modelContent = getModelContent(_modelRepoUrl, dtmiPath, _gitToken);
                    }

                    if (string.IsNullOrEmpty(modelContent))
                    {
                        modelContent = getModelContent("https://devicemodels.azure.com", dtmiPath, string.Empty);
                    }

                    if (string.IsNullOrEmpty(modelContent))
                    {
                        return false;
                    }

                    modelList.Add(modelContent);

                    try
                    {
                        await _adtClient.CreateModelsAsync(modelList);
                        log.LogInformation($"Digital Twin Model {dtmi} created");
                    }
                    catch (RequestFailedException rex)
                    {
                        log.LogError($"CreateModelsAsync: {rex.Status}:{rex.Message}");
                        return false;
                    }
                }

                // create a new twin
                try
                {
                    BasicDigitalTwin twinData = new BasicDigitalTwin
                    {
                        Id = regId,
                        Metadata = { ModelId = dtmi },
                    };

                    Response<BasicDigitalTwin> response = await _adtClient.CreateOrReplaceDigitalTwinAsync(regId, twinData);
                    log.LogInformation($"Digital Twin {response.Value.Id} (Model : {response.Value.Metadata.ModelId}) created");
                }
                catch (RequestFailedException rex)
                {
                    log.LogError($"CreateOrReplaceDigitalTwinAsync: {rex.Status}:{rex.Message}");
                    return false;
                }

            }

            return false;
        }

        private static bool IsValidDtmi(string dtmi)
        {
            // Regex defined at https://github.com/Azure/digital-twin-model-identifier#validation-regular-expressions
            Regex rx = new Regex(@"^dtmi:[A-Za-z](?:[A-Za-z0-9_]*[A-Za-z0-9])?(?::[A-Za-z](?:[A-Za-z0-9_]*[A-Za-z0-9])?)*;[1-9][0-9]{0,8}$");
            return rx.IsMatch(dtmi);
        }

        private static string DtmiToPath(string dtmi)
        {
            if (!IsValidDtmi(dtmi))
            {
                return null;
            }
            // dtmi:com:example:Thermostat;1 -> dtmi/com/example/thermostat-1.json
            return $"/{dtmi.ToLowerInvariant().Replace(":", "/").Replace(";", "-")}.json";
        }

        private static string getModelContent(string repoUrl, string dtmiPath, string gitToken)
        {
            string modelContent = string.Empty;
            Uri modelRepoUrl = new Uri(repoUrl);
            Uri fullPath = new Uri($"{modelRepoUrl}{dtmiPath}");
            string fullyQualifiedPath = fullPath.ToString();

            if (!string.IsNullOrEmpty(gitToken))
            {
                var token = $"token {gitToken}";
                _webClient.Headers.Add("Authorization", token);
            }

            try
            {
                modelContent = _webClient.DownloadString(fullyQualifiedPath);
            }
            catch (System.Net.WebException e)
            {
                Console.WriteLine($"Exception in getModelContent() : {e.Message}");
                return string.Empty;
            }

            return modelContent;
        }
    }

    public class ResponseObj
    {
        public string iotHubHostName { get; set; }
//        public TwinState initialTwin { get; set; }
    }
}

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

namespace IoT_Plug_and_Play_Workshop_Functions
{
    public static class Dps_Processor
    {
        private static readonly HttpClient _httpClient = new HttpClient();
        private static string _adtServiceUrl = Environment.GetEnvironmentVariable("ADT_HOST_URL");
        private static DigitalTwinsClient _adtClient = null;

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

        public static async Task ProcessADT(string dtmi, string regId, ILogger log)
        {

            if (string.IsNullOrEmpty(dtmi))
            {
                return;

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
                AsyncPageable<DigitalTwinsModelData> allModels = _adtClient.GetModelsAsync();
                await foreach (DigitalTwinsModelData model in allModels)
                {
                    Console.WriteLine($"Retrieved model '{model.Id}', " +
                        $"display name '{model.LanguageDisplayNames["en"]}', " +
                        $"uploaded on '{model.UploadedOn}', " +
                        $"and decommissioned '{model.Decommissioned}'");
                }
            }

        }
    }

    public class ResponseObj
    {
        public string iotHubHostName { get; set; }
//        public TwinState initialTwin { get; set; }
    }
}

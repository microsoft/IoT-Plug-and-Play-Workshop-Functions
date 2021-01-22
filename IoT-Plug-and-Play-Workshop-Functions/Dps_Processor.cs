using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Devices.Provisioning.Service;

namespace IoT_Plug_and_Play_Workshop_Functions
{
    public static class Dps_Processor
    {
        [FunctionName("Dps_Processor")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            log.LogInformation($"Request.Body: {requestBody}");
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            string registrationId = data?.deviceRuntimeContext?.registrationId;
            string message = string.Empty;

            if (registrationId == null)
            {
                message = "Registration ID not provided";
            }
            else
            {
                dynamic payload = data?.deviceRuntimeContext?.payload;
                string modelId = payload.modelId;
                log.LogInformation($"ModelId: {modelId}");
            }

            return new BadRequestObjectResult(message);
        }
    }

    public class ResponseObj
    {
        public string iotHubHostName { get; set; }
        public TwinState initialTwin { get; set; }
    }
}

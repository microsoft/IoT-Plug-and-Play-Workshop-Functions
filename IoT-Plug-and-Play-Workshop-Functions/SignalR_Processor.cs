using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;

namespace IoT_Plug_and_Play_Workshop_Functions
{
    public static class SignalR_Processor
    {
        private const string HUBNAME = "telemetryhub";

        //
        // A function in case an application (e.g. web site) wants to establish connection.
        // 
        [FunctionName("negotiate")]
        public static IActionResult GetSignalRInfo(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req,
            [SignalRConnectionInfo(HubName = HUBNAME)] SignalRConnectionInfo connectionInfo,
            ILogger log)
        {
            var headers = req.Headers;

            if (connectionInfo != null)
            {
                Microsoft.Extensions.Primitives.StringValues originValues;
                headers.TryGetValue("Origin", out originValues);
                log.LogInformation($"Request from : {originValues}");
                return new OkObjectResult(connectionInfo);
            }
            else
            {
                log.LogError("Connection Info Missing");
                return new BadRequestObjectResult("Connection Info Missing");
            }
        }

        //
        // A test function so you can make a REST call with app such as Postman to test SignalR
        // 
        [FunctionName("SignalR_Test")]
        public static async Task<IActionResult> SignalR_Test(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [SignalR(HubName = HUBNAME)] IAsyncCollector<SignalRMessage> signalRMessages,
            ILogger log)
        {
            log.LogInformation("SignalR Test Function");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            await signalRMessages.AddAsync(new SignalRMessage
            {
                Target = "SignalRTest",
                Arguments = new[] { data }
            });
            log.LogInformation("SignalR Test Function.  Message Sent.");
            return new OkObjectResult($"Received Message : {data}");
        }
    }
}
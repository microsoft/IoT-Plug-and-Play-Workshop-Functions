using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Devices.Shared;               // For TwinCollection
using Microsoft.Azure.Devices.Provisioning.Service; // For TwinState
using System.Collections.Generic;
using Microsoft.Azure.DigitalTwins.Parser;
using System.Linq;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Azure.Core.Pipeline;
using System.Net.Http;
using Azure;

namespace IoT_Plug_and_Play_Workshop_Functions
{
    public static class Dps_Processor
    {

        private static readonly string _gitToken = Environment.GetEnvironmentVariable("GitToken");
        private static readonly string _modelRepoUrl_Private = Environment.GetEnvironmentVariable("ModelRepository");
        private static string _adtServiceUrl = Environment.GetEnvironmentVariable("ADT_HOST_URL");
        private static DigitalTwinsClient _adtClient = null;
        private static ILogger _logger;
        private static DeviceModelResolver _resolver = null;
        private static readonly HttpClient _httpClient = new HttpClient();

        //private static readonly HttpClient _httpClient = new HttpClient();
        //private static readonly WebClient _webClient = new WebClient();


        //
        // Sample code to perform custom actions during device provisioning.
        // This sample uses Seeed Wio Terminal and Impinj R700
        // https://devicecatalog.azure.com/devices/8b9c5072-68fd-4fc3-8e5f-5b15e3a20bd9
        // PnP Device Model
        // https://devicemodels.azure.com/dtmi/seeedkk/wioterminal/wioterminal_aziot_example-5.expanded.json
        // https://github.com/Azure/iot-plugandplay-models/blob/main/dtmi/impinj/fixedreader-11.json
        // https://github.com/Azure/iot-plugandplay-models/blob/main/dtmi/impinj/r700-131.json
        //
        // - Add Device Twin
        // - Add Tags
        // - Create model and twin, if Azure Digital Twins is configured.
        //
        // Note : This operation takes place "Before" the target device is connected to IoT Hub.
        //
        [FunctionName("Dps_Processor")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest request,
            ILogger log)
        {
            string requestBody = await new StreamReader(request.Body).ReadToEndAsync();
            string errorMessage = string.Empty;
            DpsResponse response = new DpsResponse();
            string registrationId;
            DateTime localDate = DateTime.Now;

            _logger = log;

            dynamic requestData = JsonConvert.DeserializeObject(requestBody);

            if (requestData.ContainsKey("enrollmentGroup"))
            {
                log.LogInformation("Group Enrollment");
                registrationId = requestData?.enrollmentGroup?.enrollmentGroupId;
            }
            else
            {
                log.LogInformation("Individual Enrollment");
                registrationId = requestData?.deviceRuntimeContext?.registrationId;
            }

            string[] iothubs = requestData?.linkedHubs.ToObject<string[]>();

            log.LogInformation($"dps_processor : Request.Body: {JsonConvert.SerializeObject(requestData, Formatting.Indented)}");

            #region payload_sample
            /* Payload Example
            {
              "enrollmentGroup": {
                "enrollmentGroupId": "PVDemo-Group-Enrollment-Custom-Allocation",
                "attestation": {
                  "type": "symmetricKey"
                },
                "capabilities": {
                  "iotEdge": false
                },
                "etag": "\"1a055216-0000-0800-0000-60a637160000\"",
                "provisioningStatus": "enabled",
                "reprovisionPolicy": {
                  "updateHubAssignment": true,
                  "migrateDeviceData": true
                },
                "createdDateTimeUtc": "2021-05-20T10:15:51.2294536Z",
                "lastUpdatedDateTimeUtc": "2021-05-20T10:16:54.6543548Z",
                "allocationPolicy": "custom",
                "iotHubs": [
                  "PVDemo-IoTHub.azure-devices.net"
                ],
                "customAllocationDefinition": {
                  "webhookUrl": "https://pvdemo-functions.azurewebsites.net/api/dps_processor?****",
                  "apiVersion": "2019-03-31"
                }
              },
              "deviceRuntimeContext": {
                "registrationId": "WioTerminal",
                "symmetricKey": {},
                "payload": {
                  "modelId": "dtmi:seeedkk:wioterminal:wioterminal_aziot_example_gps;5"
                }
              },
              "linkedHubs": [
                "PVDemo-IoTHub.azure-devices.net"
              ]
            }
            */
            #endregion

            try
            {
                if (registrationId == null)
                {
                    log.LogError($"Missing Registration ID");
                }
                else if (iothubs == null)
                {
                    errorMessage = "No linked hubs for this enrollment.";
                    log.LogError("linked IoT Hub");
                }
                else
                {
                    //
                    // Select IoT Hub to assign to.
                    // For this demo, we just use the first IoT Hub.
                    response.iotHubHostName = iothubs[0];
                    //foreach (var iothub in iothubs)
                    //{
                    //    // do specifics for linked hubs
                    //    // e.g. pick up right IoT Hub based on device id
                    //}

                    // Create a new Twin Collection to manipulate Device Twin
                    TwinCollection twinTag = new TwinCollection();
                    //
                    // Add a tag for the device
                    // tags are for solution only, devices do not see tags
                    //
                    // https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-device-twins#device-twins
                    twinTag["TagFromDpsWebHook"] = "CustomAllocationSample";

                    //
                    // build initial twin (Desired Properties) for the device
                    // these values will be passed to the device during Initial Get
                    //
                    TwinCollection twinDesired = new TwinCollection();
                    twinDesired["FromDpsWebHook1"] = "InitialTwinByCustomAllocation";
                    twinDesired["FromDpsWebHook2"] = registrationId;

                    //
                    // IoT Plug and Play
                    // Check if DTDL Model Id is announced for this device
                    //
                    string componentName = string.Empty;
                    IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel = null;
                    string modelId = requestData?.deviceRuntimeContext?.payload?.modelId;

                    if (!string.IsNullOrEmpty(modelId))
                    {
                        // If DTMI is given to DPS payload, parse it.
                        parsedModel = await DeviceModelResolveAndParse(modelId);

                        await CreateTwinInAdt(modelId, registrationId, log);
                    }

                    //
                    // Set a desired property based on device model
                    // Impinj R700 has a desired property "Hostname" to set the host name of the device.
                    // For the demo purpose, we do this everytime device connects to DPS.
                    // This should be done only first time.
                    //
                    if (parsedModel != null)
                    {
                        string propertyName = "Hostname";
                        // Example : Setting Writable Property using Device Model
                        // We are interested in properties

                        // Search a writable property "Hostname"
                        DTPropertyInfo property = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Property).Select(x => x.Value as DTPropertyInfo).Where(x => x.Writable == true).Where(x => x.Name == propertyName).FirstOrDefault();

                        if (property != null)
                        {
                            // Give a host name based on timestamp
                            var dateString = $"{localDate.Year}{localDate.Month}{localDate.Day}-{localDate.Hour}{localDate.Minute}{localDate.Second}";
                            log.LogInformation($"Found Writable Property '{propertyName}'");

                            // If no match, this interface must be from Component
                            if (!modelId.Equals(property.DefinedIn.AbsoluteUri))
                            {
                                var component = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Component).Select(x => x.Value as DTComponentInfo).Where(x => x.Schema.Id.ToString() == property.ChildOf.AbsoluteUri).FirstOrDefault();
                                if (component != null)
                                {
                                    TwinCollection componentTwin = new TwinCollection();
                                    TwinCollection hostnameComponentTwin = new TwinCollection();
                                    // Hostname takes a parameter as JSON Object
                                    // JSON looks like this
                                    // "desired" : {
                                    //   "R700": {
                                    //     "__t": "c",
                                    //     "Hostname" : {
                                    //       "hostname" : "<New Name>"
                                    //     }
                                    //   }
                                    // }
                                    if (property.Schema.EntityKind == DTEntityKind.Object)
                                    {
                                        DTObjectInfo parameterObj = property.Schema as DTObjectInfo;
                                        hostnameComponentTwin[parameterObj.Fields[0].Name] = $"impinj-{dateString}";
                                        componentTwin[property.Name] = hostnameComponentTwin;
                                        componentTwin["__t"] = "c";
                                        twinDesired[component.Name] = componentTwin;
                                    }
                                }
                            }
                            else
                            {
                                twinDesired[property.Name] = $"impinj-{dateString}";
                            }

                        }
                    }

                    //
                    // Create a new Twin State to respond back to DPS to complete provisioning
                    //
                    TwinState twinState = new TwinState(twinTag, twinDesired);
                    response.initialTwin = twinState;
                }
            }
            catch (Exception ex)
            {
                log.LogError($"Exception {ex}");
                errorMessage = ex.Message;
            }

            if (!string.IsNullOrEmpty(errorMessage))
            {
                log.LogError($"Error : {errorMessage}");
                return new BadRequestObjectResult(errorMessage);
            }

            log.LogInformation($"Response to DPS \r\n {JsonConvert.SerializeObject(response)}");
            return (ActionResult)new OkObjectResult(response);
        }

        private static async Task<IReadOnlyDictionary<Dtmi, DTEntityInfo>> DeviceModelResolveAndParse(string dtmi)
        {
            if (!string.IsNullOrEmpty(dtmi))
            {
                try
                {
                    if (_resolver == null)
                    {
                        _resolver = new DeviceModelResolver(_modelRepoUrl_Private, _gitToken, _logger);
                    }

                    // resolve and parse device model
                    return await _resolver.ParseModelAsync(dtmi);

                }
                catch (Exception e)
                {
                    _logger.LogError($"Error Resolve(): {e.Message}");
                }
            }

            return null;
        }

        private static async Task<bool> CreateTwinInAdt(string dtmi, string regId, ILogger log)
        {
            if (string.IsNullOrEmpty(dtmi) || string.IsNullOrEmpty(regId))
            {
                return false;
            }
            else if (_adtClient == null && !string.IsNullOrEmpty(_adtServiceUrl))
            {
                try
                {

                    //DefaultAzureCredential cred = new DefaultAzureCredential(
                    //    new DefaultAzureCredentialOptions { ManagedIdentityClientId = "https://digitaltwins.azure.net" }
                    //    );

                    //_adtClient = new DigitalTwinsClient(new Uri(_adtServiceUrl), cred);

                    var credentials = new DefaultAzureCredential();
                    _adtClient = new DigitalTwinsClient(new Uri(_adtServiceUrl), credentials, new DigitalTwinsClientOptions { Transport = new HttpClientTransport(_httpClient) });
                    log.LogInformation("ADT service client connection created.");
                }
                catch (Exception e)
                {
                    log.LogError($"ADT service client connection failed. {e}");
                    return false;
                }
            }

            if (_adtClient != null)
            {
                bool bFoundTwin = false;
                bool bFoundModel = false;

                try
                {

                    string query = $"SELECT * FROM DigitalTwins T WHERE $dtId = '{regId}' AND IS_OF_MODEL('{dtmi}')";
                    AsyncPageable<BasicDigitalTwin> asyncPageableResponse = _adtClient.QueryAsync<BasicDigitalTwin>(query);

                    await foreach (BasicDigitalTwin twin in asyncPageableResponse)
                    {
                        // Get DT ID from the Twin
                        log.LogInformation($"Twin '{twin.Id}' with Registration ID '{regId}' found in DT");
                        bFoundTwin = true;
                    }

                    if (bFoundTwin == false)
                    {
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
                    }
                }
                catch (RequestFailedException rex)
                {
                    log.LogError($"GetModelAsync: {rex.Status}:{rex.Message}");
                    return false;
                }

            }

            return true;

            //    if (_adtClient != null)
            //    {
            //        bool bFoundTwin = false;
            //        bool bFoundModel = false;
            //        // check if twin exists for this device
            //        try
            //        {

            //            string query = $"SELECT * FROM DigitalTwins T WHERE $dtId = '{regId}' AND IS_OF_MODEL('{dtmi}')";
            //            AsyncPageable<BasicDigitalTwin> asyncPageableResponse = _adtClient.QueryAsync<BasicDigitalTwin>(query);

            //            await foreach (BasicDigitalTwin twin in asyncPageableResponse)
            //            {
            //                // Get DT ID from the Twin
            //                log.LogInformation($"Twin '{twin.Id}' with Registration ID '{regId}' found in DT");
            //                bFoundTwin = true;
            //            }
            //        }
            //        catch (RequestFailedException rex)
            //        {
            //            log.LogError($"GetModelAsync: {rex.Status}:{rex.Message}");
            //            return false;
            //        }

            //        if (bFoundTwin)
            //        {
            //            log.LogInformation($"Twin found. ID: {regId} (Model: {dtmi})");
            //            return true;
            //        }

            //        AsyncPageable<DigitalTwinsModelData> allModels = _adtClient.GetModelsAsync();
            //        await foreach (DigitalTwinsModelData model in allModels)
            //        {

            //            if (model.Id.Equals(dtmi))
            //            {
            //                log.LogInformation($"Found model ID : {dtmi}");
            //                bFoundModel = true;
            //                break;
            //            }
            //        }

            //        //if (!bFoundModel)
            //        //{
            //        //    // create a model
            //        //    // 1. Get model definition
            //        //    string modelContent = string.Empty;
            //        //    var modelList = new List<string>();
            //        //    string dtmiPath = DtmiToPath(dtmi.ToString());


            //        //    // if private repo is provided, resolve model with private repo first.
            //        //    if (!string.IsNullOrEmpty(_modelRepoUrl))
            //        //    {
            //        //        modelContent = getModelContent(_modelRepoUrl, dtmiPath, _gitToken);
            //        //    }

            //        //    if (string.IsNullOrEmpty(modelContent))
            //        //    {
            //        //        modelContent = getModelContent("https://devicemodels.azure.com", dtmiPath, string.Empty);
            //        //    }

            //        //    if (string.IsNullOrEmpty(modelContent))
            //        //    {
            //        //        return false;
            //        //    }

            //        //    modelList.Add(modelContent);

            //        //    try
            //        //    {
            //        //        await _adtClient.CreateModelsAsync(modelList);
            //        //        log.LogInformation($"Digital Twin Model {dtmi} created");
            //        //    }
            //        //    catch (RequestFailedException rex)
            //        //    {
            //        //        log.LogError($"CreateModelsAsync: {rex.Status}:{rex.Message}");
            //        //        return false;
            //        //    }
            //        }

            //        // create a new twin
            //        try
            //        {
            //            BasicDigitalTwin twinData = new BasicDigitalTwin
            //            {
            //                Id = regId,
            //                Metadata = { ModelId = dtmi },
            //            };

            //            Response<BasicDigitalTwin> response = await _adtClient.CreateOrReplaceDigitalTwinAsync(regId, twinData);
            //            log.LogInformation($"Digital Twin {response.Value.Id} (Model : {response.Value.Metadata.ModelId}) created");
            //        }
            //        catch (RequestFailedException rex)
            //        {
            //            log.LogError($"CreateOrReplaceDigitalTwinAsync: {rex.Status}:{rex.Message}");
            //            return false;
            //        }

            //    }

            //    return false;
            //}

        }

        public class DpsResponse
        {
            public string iotHubHostName { get; set; }
            public TwinState initialTwin { get; set; }
        }
    }
}

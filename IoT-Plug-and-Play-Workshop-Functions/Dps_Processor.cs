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
        private static readonly string _gitToken = Environment.GetEnvironmentVariable("PRIVATE_MODEL_REPOSIROTY_TOKEN");
        private static readonly string _modelRepoUrl_Private = Environment.GetEnvironmentVariable("PRIVATE_MODEL_REPOSIROTY_URL");
        private static string _adtServiceUrl = Environment.GetEnvironmentVariable("ADT_HOST_URL");
        private static DigitalTwinsClient _adtClient = null;
        private static ILogger _logger = null;
        private static DeviceModelResolver _resolver = null;
        private static readonly HttpClient _httpClient = new HttpClient();

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
            ILogger logger)
        {
            string requestBody = await new StreamReader(request.Body).ReadToEndAsync();
            string errorMessage = string.Empty;
            DpsResponse response = new DpsResponse();
            string registrationId;
            DateTime localDate = DateTime.Now;

            _logger = logger;

            dynamic requestData = JsonConvert.DeserializeObject(requestBody);

            _logger.LogInformation(requestBody);

            registrationId = requestData?.deviceRuntimeContext?.registrationId;

            if (requestData.ContainsKey("enrollmentGroup"))
            {
                _logger.LogInformation("Group Enrollment");
                //registrationId = requestData?.enrollmentGroup?.enrollmentGroupId;
            }
            else
            {
                _logger.LogInformation("Individual Enrollment");
                //registrationId = requestData?.deviceRuntimeContext?.registrationId;
            }

            _logger.LogInformation($"Registration Id : {registrationId}");

            string[] iothubs = requestData?.linkedHubs.ToObject<string[]>();

            _logger.LogInformation($"dps_processor : Request.Body: {JsonConvert.SerializeObject(requestData, Formatting.Indented)}");

            #region payload_sample
            /* Payload Example
            {
              "enrollmentGroup": {
                "enrollmentGroupId": "SAS-IoT-Devices",
                "attestation": {
                  "type": "symmetricKey"
                },
                "etag": "\"3206e072-0000-0800-0000-60ffc1960000\"",
                "provisioningStatus": "enabled",
                "createdDateTimeUtc": "2021-07-27T08:19:34.2864775Z",
                "lastUpdatedDateTimeUtc": "2021-07-27T08:19:34.2864775Z",
                "allocationPolicy": "custom",
                "customAllocationDefinition": {
                  "webhookUrl": "https://iotpnpws-functions-20210726.azurewebsites.net/api/Dps_Processor?****",
                  "apiVersion": "2019-03-31"
                }
              },
              "deviceRuntimeContext": {
                "registrationId": "SAS-IoT-Devices",
                "currentIotHubHostName": "IoTPnPWS-Hub-20210726.azure-devices.net",
                "currentDeviceId": "SAS-IoT-Devices",
                "symmetricKey": {},
                "payload": {
                  "iotcModelId": "dtmi:azureiot:PhoneAsADevice;2",
                  "modelId": "dtmi:azureiot:PhoneAsADevice;2"
                }
              },
              "linkedHubs": [
                "IoTPnPWS-Hub-20210726.azure-devices.net"
              ]
            }
            */
            #endregion

            try
            {
                if (registrationId == null)
                {
                    _logger.LogError($"Missing Registration ID");
                }
                else if (iothubs == null)
                {
                    errorMessage = "No linked hubs for this enrollment.";
                    _logger.LogError("linked IoT Hub");
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
                        _logger.LogInformation($"Model ID : {modelId}");

                        // If DTMI is given to DPS payload, parse it.
                        parsedModel = await DeviceModelResolveAndParse(modelId);

                        await ProcessDigitalTwin(modelId, parsedModel, registrationId);
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
                            _logger.LogInformation($"Found Writable Property '{propertyName}'");

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
                _logger.LogError($"Exception {ex}");
                errorMessage = ex.Message;
            }

            if (!string.IsNullOrEmpty(errorMessage))
            {
                _logger.LogError($"Error : {errorMessage}");
                return new BadRequestObjectResult(errorMessage);
            }

            _logger.LogInformation($"Response to DPS \r\n {JsonConvert.SerializeObject(response)}");
            return (ActionResult)new OkObjectResult(response);
        }

        /// <summary>
        /// Resolve IoT Plug and Play device model and parse the model
        /// </summary>
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
                    _logger.LogError($"Error DeviceModelResolveAndParse(): {e.Message}");
                }
            }

            return null;
        }

        /// <summary>
        /// Process Digital Twin
        /// Search digital twin for this device.
        /// If not exist, search digital twin model
        /// If the model does not exist, create a model, then create digital twin
        /// </summary>
        private static async Task<bool> ProcessDigitalTwin(string dtmi, IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel, string regId)
        {
            bool bFoundTwin = false;

            if (string.IsNullOrEmpty(dtmi) || string.IsNullOrEmpty(regId))
            {
                return false;
            }
            else if (_adtClient == null && !string.IsNullOrEmpty(_adtServiceUrl))
            {
                // Create a digital twin client
                try
                {
                    //DefaultAzureCredential cred = new DefaultAzureCredential(
                    //    new DefaultAzureCredentialOptions { ManagedIdentityClientId = "https://digitaltwins.azure.net" }
                    //    );

                    //_adtClient = new DigitalTwinsClient(new Uri(_adtServiceUrl), cred);

                    var credentials = new DefaultAzureCredential();
                    _adtClient = new DigitalTwinsClient(new Uri(_adtServiceUrl), credentials, new DigitalTwinsClientOptions { Transport = new HttpClientTransport(_httpClient) });
                    _logger.LogInformation("ADT service client connection created.");
                }
                catch (Exception e)
                {
                    _logger.LogError($"ADT service client connection failed. {e}");
                }
            }

            if (_adtClient != null)
            {
                bool bFoundModel = false;

                try
                {
                    // Check to see a digital twin for this device exists or not
                    bFoundTwin = await FindTwinFromDeviceId(_adtClient, dtmi, regId);

                    if (bFoundTwin == false)
                    {
                        _logger.LogInformation($"Digital Twin '{regId}' not found");

                        // Check to see if a digital twin model for this DTMI exists or not
                        bFoundModel = await FindTwinModel(_adtClient, dtmi);

                        if (bFoundModel == false)
                        {
                            // Digital Twin model does not exist.  Create one.
                            //_logger.LogInformation($"Twin Model {dtmi} not found");
                            bFoundModel = await CreateTwinModel(_adtClient, parsedModel, dtmi);
                        }
                        else
                        {
                            _logger.LogInformation($"Twin Model {dtmi} found");
                        }

                        if (bFoundModel == true)
                        {
                            // Digital Twin model already exists.  Create digital twin.
                            bFoundTwin = await CreateDigitalTwin(_adtClient, parsedModel, dtmi, regId);
                        }
                    }
                    else 
                    {
                    
                    }
                }
                catch (RequestFailedException rex)
                {
                    _logger.LogError($"ProcessDigitalTwin: {rex.Status}:{rex.Message}");
                }
            }

            return bFoundTwin;
        }

        /// <summary>
        /// Find Digital Twin in ADT
        /// </summary>
        private static async Task<bool> FindTwinFromDeviceId(DigitalTwinsClient dtClient, string dtmi, string deviceId)
        {
            bool bFound = false;
            try
            {
                string query = $"SELECT * FROM DigitalTwins T WHERE $dtId = '{deviceId}' AND IS_OF_MODEL('{dtmi}')";
                AsyncPageable<BasicDigitalTwin> asyncPageableResponse = dtClient.QueryAsync<BasicDigitalTwin>(query);

                await foreach (BasicDigitalTwin twin in asyncPageableResponse)
                {
                    // Get DT ID from the Twin
                    _logger.LogInformation($"Twin '{twin.Id}' with Registration ID '{deviceId}' found in DT");
                    bFound = true;
                    break;
                }

                if (bFound == false)
                {
                    _logger.LogInformation($"Twin '{deviceId}' not found");
                }
            }
            catch (RequestFailedException rex)
            {
                _logger.LogError($"FindTwinFromDeviceId: {rex.Status}:{rex.Message}");
            }

            return bFound;
        }
 
        /// <summary>
        /// Find Digital Twin Model in ADT
        /// </summary>
        private static async Task<bool> FindTwinModel(DigitalTwinsClient dtClient, string dtmi)
        {
            bool bFound = false;
            try
            {
                AsyncPageable<DigitalTwinsModelData> dtModels = dtClient.GetModelsAsync();

                await foreach (DigitalTwinsModelData dtModel in dtModels)
                {

                    if (dtModel.Id.Equals(dtmi))
                    {
                        _logger.LogInformation($"Found model ID : {dtmi}");
                        bFound = true;
                        break;
                    }
                }

                if (bFound == false)
                {
                    _logger.LogInformation($"Twin Model '{dtmi}' not found");
                }
            }
            catch (RequestFailedException rex)
            {
                _logger.LogError($"FindTwinModel: {rex.Status}:{rex.Message}");
            }

            return bFound;
        }

        /// <summary>
        /// Create digital twin model in Azure Digital Twins
        /// </summary>
        private static async Task<bool> CreateTwinModel(DigitalTwinsClient dtClient, IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel, string dtmi)
        {
            bool bCreated = false;
            string dtmiPath = string.Empty;
            var modelList = new List<string>();
            var modelContentList = new List<string>();

            try
            {
                if (_resolver == null)
                {
                    _resolver = new DeviceModelResolver(_modelRepoUrl_Private, _gitToken, _logger);
                }

                if (_resolver == null)
                {
                    return bCreated;
                }

                string modelContent = string.Empty;

                // check to see if this model contains components

                var components = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Component).ToList();

                if (components != null)
                {
                    // Add Components' DTMI to the list.
                    foreach (var component in components)
                    {
                        DTComponentInfo compInfo = component.Value as DTComponentInfo;
                        var compId = compInfo.Schema.Id.AbsoluteUri;
                        var bFound = await FindTwinModel(dtClient, compId);

                        if (bFound == false)
                        {
                            modelList.Add(compInfo.Schema.Id.AbsoluteUri);
                        }
                    }
                }

                // Add model id to the list
                modelList.Add(dtmi);

                foreach (var modelId in modelList)
                {
                    // Create a path from DTMI
                    dtmiPath = _resolver.DtmiToPath(modelId);

                    // Retrieve Device Model contents (JSON)
                    // if private repo is provided, resolve model with private repo first.
                    if (!string.IsNullOrEmpty(_modelRepoUrl_Private))
                    {
                        _logger.LogInformation($"Searching Model in Private Model repo {_modelRepoUrl_Private}");
                        modelContent = await _resolver.GetModelContentAsync(dtmiPath, _modelRepoUrl_Private);
                    }

                    // if not found in the private model repository, try public repository
                    if (string.IsNullOrEmpty(modelContent))
                    {
                        _logger.LogInformation($"Searching Model in Public Model repo");
                        modelContent = await _resolver.GetModelContentAsync(dtmiPath, "https://devicemodels.azure.com");
                    }

                    if (!string.IsNullOrEmpty(modelContent))
                    {
                        //_logger.LogInformation($"Model Content {modelContent}");

                        modelContentList.Add(modelContent);
                    }
                    else
                    {
                        _logger.LogWarning($"Device Model content for {dtmi} not found");
                        break;
                    }
                }

                if (modelContentList.Count > 0)
                {
                    var result = await _adtClient.CreateModelsAsync(modelContentList);

                    var httpResponse = result.GetRawResponse();


                    if (httpResponse.Status == 201)
                    {
                        bCreated = true;
                    }
                }
                else
                {
                    _logger.LogWarning($"Device Model Definition file for {dtmi} not found");
                }
            }
            catch (RequestFailedException rex)
            {
                _logger.LogError($"CreateTwinModel: {rex.Status}:{rex.Message}");
            }

            return bCreated;
        }

        /// <summary>
        /// Create Digital Twin for a new device
        /// </summary>
        private static async Task<bool> CreateDigitalTwin(DigitalTwinsClient dtClient, IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel, string dtmi, string deviceId)
        {
            bool bCreated = false;

            try
            {
                BasicDigitalTwin twinData = new BasicDigitalTwin
                {
                    Id = deviceId,
                    // model Id of digital twin
                    Metadata = { ModelId = dtmi },
                    Contents = {},
                };

                var components = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Component).ToList();

                if (components.Count > 0)
                {
                    foreach(var component in components)
                    {
                        DTComponentInfo dtComp = component.Value as DTComponentInfo;
                        twinData.Contents.Add(dtComp.Name, new BasicDigitalTwinComponent { });
                    }
                }

                Response<BasicDigitalTwin> response = await _adtClient.CreateOrReplaceDigitalTwinAsync(deviceId, twinData);

                var result = response.GetRawResponse();

                if ( result.Status >= 200 && result.Status <= 299)
                {
                    _logger.LogInformation($"Digital Twin {response.Value.Id} (Model : {response.Value.Metadata.ModelId}) created");
                    bCreated = true;
                }
            }
            catch (RequestFailedException rex)
            {
                _logger.LogError($"CreateDigitalTwin: {rex.Status}:{rex.Message}");
            }

            return bCreated;
        }

        public class DpsResponse
        {
            public string iotHubHostName { get; set; }
            public TwinState initialTwin { get; set; }
        }
    }
}

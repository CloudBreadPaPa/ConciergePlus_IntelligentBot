using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using System.Diagnostics;
using System.Configuration;
using Newtonsoft.Json;


using System.Globalization;
using System.Net.Http.Headers;
using System.Net.Http;
using System.Web;
using System.Linq;

namespace ChatMessageProcessor
{
    internal class SentimentEventProcessor : IEventProcessor
    {
        private readonly string _chatTopicPath = ConfigurationManager.AppSettings["chatTopicPath"];
        private readonly string _connectionString = ConfigurationManager.AppSettings["eventHubConnectionString"];
        private readonly string _serviceBusConnectionString = ConfigurationManager.AppSettings["serviceBusConnectionString"];
        private readonly string _destinationEventHubName = ConfigurationManager.AppSettings["destinationEventHubName"];
        private readonly string _textAnalyticsBaseUrl = ConfigurationManager.AppSettings["textAnalyticsBaseUrl"];
        private readonly string _textAnalyticsAccountKey = ConfigurationManager.AppSettings["textAnalyticsAccountKey"];

        // TODO: Update the LUIS base URL to the one assigned to your app
        private readonly string _luisBaseUrl = "https://westus.api.cognitive.microsoft.com/";
        private readonly string _luisQueryParams = "luis/v2.0/apps/{0}?subscription-key={1}&q={2}";
        private readonly string _luisAppId = ConfigurationManager.AppSettings["luisAppId"];
        private readonly string _luisKey = ConfigurationManager.AppSettings["luisKey"];

        private Stopwatch _checkpointStopWatch;
        private TopicClient _topicClient;
        private EventHubClient _eventHubClient;

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine("Processor Shutting Down. Partition '{0}', Reason '{1}'.", context.Lease.PartitionId, reason);
            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
            }
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            Console.WriteLine("SimpleEventProcessor initialized. Partition '{0}', Offset '{1}'", context.Lease.PartitionId, context.Lease.Offset);
            this._checkpointStopWatch = new Stopwatch();
            this._checkpointStopWatch.Start();

            this._topicClient = TopicClient.CreateFromConnectionString(_serviceBusConnectionString, _chatTopicPath);
            this._eventHubClient = EventHubClient.CreateFromConnectionString(_connectionString, _destinationEventHubName);

            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var eventData in messages)
            {
                try
                {
                    //TODO: 1.Extract the JSON payload from the binary message
                    var eventBytes = // Get Bytes from event data 
                    var jsonMessage = //Get UTF 8 string from event bytes 
                    Console.WriteLine("Message Received. Partition '{0}', SessionID '{1}' Data '{2}'", context.Lease.PartitionId, eventData.Properties["SessionId"], jsonMessage);

                    //TODO: 2.Deserialize the JSON message payload into an instance of MessageType
                    var msgObj = JsonConvert.DeserializeObject< /*Complete This*/ >(/*Complete This*/);

                    //TODO: 12 Append sentiment score to chat message object
                    //Add a line here that invokes GetSentimentScore and sets the score on the msg object

                    //TODO: 3.Create a BrokeredMessage (for Service Bus) and EventData instance (for EventHubs) from source message body
                    var updatedEventBytes = // Get bytes from msgObj  
                    BrokeredMessage chatMessage = // Create a new brokered message from the updated event bytes
                    EventData updatedEventData = //Create a new event data object from the updated event bytes

                    //TODO: 4.Copy the message properties from source to the outgoing message instances
                    foreach (var prop in eventData.Properties)
                    {
                        chatMessage.Properties.Add(/*Complete This*/, /*Complete This*/);
                        updatedEventData.Properties.Add(/*Complete This*/, /*Complete This*/);
                    }

                    //TODO: 5.Send chat message to Topic
                    _topicClient.Send(/*Complete This*/);
                    Console.WriteLine("Forwarded message to topic.");

                    //TODO: 6.Send chat message to next EventHub (for archival)
                    _eventHubClient.Send(/*Complete This*/);
                    Console.WriteLine("Forwarded message to event hub.");

                    //TODO: 13.Respond to chat message intent if appropriate
                    //var intent = await GetIntentAndEntities(msgObj.message);
                    //HandleIntent(intent, msgObj);
                }
                catch (Exception ex)
                {
                    LogError(ex.Message);
                }
            }

            if (_checkpointStopWatch.Elapsed > TimeSpan.FromSeconds(5))
            {
                await context.CheckpointAsync();
                _checkpointStopWatch.Restart();
            }
        }

        private async Task<double> GetSentimentScore(string messageText)
        {
            double sentimentScore = -1;
            using (var client = new HttpClient())
            {

                //TODO: 7.Configure the HTTPClient base URL and request headers
                //client.BaseAddress = new Uri(/* Complete this with base url to Text API */);
                //client.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", /* Complete this with Key to Text API  */);
                //client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                //TODO: 8.Construct a sentiment request object 
                //var req = new SentimentRequest()
                //{
                //    /* Complete this with a single document having id of 1 and text of the message */
                //};

                //TODO: 9.Serialize the request object to a JSON encoded in a byte array
                //var jsonReq = //Complete this...  
                //byte[] byteData = //Complete this...get byte array from jsonReq 

                //TODO: 10.Post the request to the /sentiment endpoint
                //string uri = "sentiment";
                //string jsonResponse = "";
                //using (var content = new ByteArrayContent(byteData))
                //{
                //    content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                //    var sentimentResponse = //Complete this... do a Post using the uri and content 
                //    jsonResponse = //Complete this...extract the response content as a string 
                //}
                //Console.WriteLine("\nDetect sentiment response:\n" + jsonResponse);

                //TODO: 11.Deserialize sentiment response and extract the score
                //var result = JsonConvert.DeserializeObject<SentimentResponse>(jsonResponse);
                //sentimentScore = //Complete this...retrieve the score for the first document in the result

            }
            return sentimentScore;
        }

        private void HandleIntent(LuisResponse intent, MessageType msgObj)
        {
            var primaryIntent = intent.topScoringIntent;
            var primaryEntity = intent.entities.FirstOrDefault();
            if (primaryIntent != null && primaryEntity != null)
            {
                if (primaryIntent.intent.Equals("OrderIn") && primaryIntent.score > 0.75)
                {
                    //Detected an actionable request with an identified entity
                    if (primaryEntity != null && primaryEntity.score > 0.5)
                    {
                        String destination = primaryEntity.type.Equals("RoomService::FoodItem") ? "Room Service" : "Housekeeping";
                        String generatedMessage = string.Format("We've sent your request for {0} to {1}, we will confirm it shortly.", primaryEntity.entity, destination);
                        SendBotMessage(msgObj, generatedMessage);
                    }
                    else
                    {
                        //Detected only an actionable request, but no entity
                        String generatedMessage = "We've received your request for service, our staff will followup momentarily.";
                        SendBotMessage(msgObj, generatedMessage);
                    }
                }
            }
        }

        private void SendBotMessage(MessageType msgObj, string generatedMessage)
        {
            MessageType generatedMsg = new MessageType()
            {
                createDate = DateTime.UtcNow,
                message = generatedMessage,
                messageId = Guid.NewGuid().ToString(),
                score = 0.5,
                sessionId = msgObj.sessionId,
                username = "ConciergeBot"
            };
            var generatedMessageBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(generatedMsg));
            BrokeredMessage botMessage = new BrokeredMessage(generatedMessageBytes);
            botMessage.Properties.Add("SessionId", msgObj.sessionId);
            _topicClient.Send(botMessage);
            Console.WriteLine("Sent bot message to topic.");
        }


        private async Task<LuisResponse> GetIntentAndEntities(string messageText)
        {
            LuisResponse result = null;
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(_luisBaseUrl);
                string queryUri = string.Format(_luisQueryParams, _luisAppId, _luisKey, Uri.EscapeDataString(messageText));
                HttpResponseMessage response = await client.GetAsync(queryUri);
                string res = await response.Content.ReadAsStringAsync();
                result = JsonConvert.DeserializeObject<LuisResponse>(res);

                Console.WriteLine("\nLUIS Response:\n" + res);
            }
            return result;
        }

        private static void LogError(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("{0} > Exception {1}", DateTime.Now, message);
            Console.ResetColor();
        }

        #region Application Data Structures
        class MessageType
        {
            public string message;
            public DateTime createDate;
            public string username;
            public string sessionId;
            public string messageId;
            public double score;
        }

        //{"documents":[{"score":0.8010351,"id":"1"}],"errors":[]}
        class SentimentResponse
        {
            public SentimentResponseDocument[] documents;
            public string[] errors;
        }
        class SentimentResponseDocument
        {
            public double score;
            public string id;
        }

        class SentimentRequest
        {
            public SentimentDocument[] documents;
        }

        class SentimentDocument
        {
            public string id;
            public string text;
        }

        class LuisResponse
        {
            public string query;
            public Intent topScoringIntent;
            public Intent[] intents;
            public LuisEntity[] entities;
        }

        class Intent
        {
            public string intent;
            public double score;
        }

        class LuisEntity
        {
            public string entity;
            public string type;
            public int startIndex;
            public int endIndex;
            public double score;
        }

        #endregion
    }
}
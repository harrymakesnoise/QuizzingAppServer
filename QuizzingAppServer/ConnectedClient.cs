using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Bson;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using AesEverywhere;
using System.Reflection;
using System.Linq;

namespace QuizzingAppServer
{
  public class ConnectedClient
  {
    public ConnectedClient(int socketId, WebSocket socket, TaskCompletionSource<object> taskCompletion)
    {
      SocketId = socketId;
      Socket = socket;
      TaskCompletion = taskCompletion;
      SessionId = "";
      AvatarURL = "";
      UserId = 0;
      SiteId = 0;
      IsAuthed = false;
      IsAdmin = false;
      IsMuted = false;
      MuteReason = "";
      IsBanned = false;
      LastScoreAwarded = 0;
      LastScoresCorrect = new JArray();
      BanReason = "";
      BroadcastQueue = Channel.CreateUnbounded<string>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    }

    public ConcurrentDictionary<string, JObject> AuthClient(JObject message)
    {

      var sessionId = "";
      var localGameId = (long)0;
      var authGameId = (long)0;
      var userAuthToken = "";
      var teamId = (long)0;

      if (message.ContainsKey("userAuth"))
      {
        userAuthToken = message["userAuth"].ToString();
      }

      if (userAuthToken != null)
      {
        AES256 aes = new AES256();
        string rawEnc = aes.Decrypt(userAuthToken, "HTRQUIZ");
        //Console.WriteLine(rawEnc);

        if (rawEnc != null)
        {
          string[] authParts = rawEnc.Split("***");
          var count = 0;
          foreach (string value in authParts)
          {
            if (count == 0)
            {
              sessionId = value;
            }
            else if (count == 1)
            {
              teamId = long.Parse(value);
            }
            else if (count == 2)
            {
              authGameId = long.Parse(value);
            }
            count++;
          }
        }
        else
        {
          Console.WriteLine("Couldn\'t decrypt string");
          return WriteClientError("Invalid auth token. Reload the page to try again.", true);
        }
      } else
      {
        Console.WriteLine("No auth string");
        return WriteClientError("Invalid auth token. Reload the page to try again.", true);
      }

      if (message.ContainsKey("gameid"))
      {
        localGameId = long.Parse(message["gameid"].ToString());
      }

      if (localGameId != authGameId)
      {
        Console.WriteLine("Game id passed not same as game id in auth token");
        return WriteClientError("Invalid game selected. Reload the page to try again.", true);
      }

      if (sessionId != "" && sessionId != null)
      {
        var dbClient = new MongoClient("mongodb://quizUser:D1g14v1t%2F*Quizz35@localhost:27017/?authSource=quiz&readPreference=primary&retryWrites=true&w=majority");
        var database = dbClient.GetDatabase("quiz");
        var collection = database.GetCollection<BsonDocument>("user_sessions");
        var filter = Builders<BsonDocument>.Filter.Eq("sessionid", sessionId);
        filter &= Builders<BsonDocument>.Filter.Eq("userid", teamId);
        var userData = collection.Find(filter).FirstOrDefault();
        if (userData != null)
        {
          AES256 aes = new AES256();
          SessionId = userData["sessionid"].ToString();
          UserId = userData["userid"].ToInt64();
          SiteId = userData["siteid"].ToInt64();
          IsAdmin = userData.Contains("isadmin") && userData["isadmin"].ToString().ToLower() == "true";
          TeamName = aes.Decrypt(userData["teamname"].ToString(), "HTRQUIZ");
          IsQuizMaster = userData.Contains("isquizmaster") && userData["isquizmaster"].ToString().ToLower() == "true";
          IsMuted = userData.Contains("ismuted") && userData["ismuted"].ToString().ToLower() == "true";
          IsBanned = userData.Contains("isbanned") && userData["isbanned"].ToString().ToLower() == "true";
          MuteReason = (userData.Contains("mutereason") ? aes.Decrypt(userData["mutereason"].ToString(), "HTRQUIZ") : "");
          BanReason = (userData.Contains("banreason") ? aes.Decrypt(userData["banreason"].ToString(), "HTRQUIZ") : "");
          GameId = localGameId;
          AvatarURL = (userData.Contains("avatar") ? aes.Decrypt(userData["avatar"].ToString(), "HTRQUIZ") : "no-avatar.png");

          if (IsBanned)
          {
            Console.WriteLine($"Socket {SocketId}: is banned.");
            return WriteClientError("Player banned:" + BanReason, true);
          }
          if (IsAdmin)
          {
            Console.WriteLine($"Socket {SocketId}: is admin");
          }
          if (IsQuizMaster)
          {
            Console.WriteLine($"Socket {SocketId}: is quiz master");
          }

          if (!WebSocketMiddleware.QuestionAnswerCount.ContainsKey(SiteId))
          {
            WebSocketMiddleware.QuestionAnswerCount.TryAdd(SiteId, new ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, long>>>());
          }
          if (!WebSocketMiddleware.QuestionAnswerCount[SiteId].ContainsKey(GameId))
          {
            WebSocketMiddleware.QuestionAnswerCount[SiteId].TryAdd(GameId, new ConcurrentDictionary<long, ConcurrentDictionary<long, long>>());
          }

          if (!WebSocketMiddleware.AnsweredQuestions.ContainsKey(SiteId))
          {
            WebSocketMiddleware.AnsweredQuestions.TryAdd(SiteId, new ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, bool>>>>());
          }
          if (!WebSocketMiddleware.AnsweredQuestions[SiteId].ContainsKey(GameId))
          {
            WebSocketMiddleware.AnsweredQuestions[SiteId].TryAdd(GameId, new ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, bool>>>());
          }

          return new ConcurrentDictionary<string, JObject>();
        } else
        {
          Console.WriteLine("No userdata found");
          return WriteClientError("No userdata found. Please logout and log back in.", true);
        }
      } else
      {
        Console.WriteLine("No session id");
        return WriteClientError("Server error. Please logout and log back in.", true);
      }
      return WriteClientError("Unknown error occurred. Please log out and log back in", true);
    }

    public ConcurrentDictionary<string, JObject> WriteClientError(string errorMessage, bool clientShouldDisconnect = false)
    {
      ConcurrentDictionary<string, JObject> messageOutput = new ConcurrentDictionary<string, JObject>();
      JObject errorMessageObject = new JObject(new JProperty("type", "error"), new JProperty("message", errorMessage), new JProperty("disconnect", clientShouldDisconnect));

      messageOutput.TryAdd("sendToSelf", errorMessageObject);

      return messageOutput;
    }

    public async Task<ConcurrentDictionary<string, JObject>> HandleMessageAsync(JObject message, string messageType, string messageCmd, JToken messageData, JToken messageUser, string messageSiteId)
    {
      try
      {
        ConcurrentDictionary<string, JObject> messageOutput = new ConcurrentDictionary<string, JObject>();

        if (messageType != null)
        {
          if (messageType == "game-event")
          {
            if (!IsQuizMaster && !IsAdmin)
              return WriteClientError("Permission denied");

            if (messageCmd == "start-game")
            {
              if (WebSocketMiddleware.LoadedQuizzes.ContainsKey(SiteId) && WebSocketMiddleware.LoadedQuizzes[SiteId].ContainsKey("full") && WebSocketMiddleware.LoadedQuizzes[SiteId]["full"].ContainsKey(GameId))
              {
                messageOutput.TryAdd("sendToQuizMaster", new JObject(new JProperty("gametimeline", WebSocketMiddleware.LoadedQuizzes[SiteId]["full"][GameId])));
              }

              if (WebSocketMiddleware.LoadedQuizzes.ContainsKey(SiteId) && WebSocketMiddleware.LoadedQuizzes[SiteId].ContainsKey("min") && WebSocketMiddleware.LoadedQuizzes[SiteId]["min"].ContainsKey(GameId))
              {
                messageOutput.TryAdd("sendToPlayers", new JObject(new JProperty("gametimeline", WebSocketMiddleware.LoadedQuizzes[SiteId]["min"][GameId])));
              }
              if (messageOutput.ContainsKey("sendToQuizMaster") && messageOutput.ContainsKey("sendToPlayers"))
              {
                WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("started", true), message);
                return messageOutput;
              }
              return WriteClientError("Game data not found");
            }
            else if (messageCmd == "end-game")
            {
              var expiryTime = DateTime.Now.AddHours(01);
              WebSocketMiddleware.CurrentGameData[SiteId][GameId]["expirytime"] = expiryTime.ToString();
              Console.WriteLine("Site " + SiteId + " / Game " + GameId + ": Expiry Time set - " + expiryTime.ToString());
              WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("ended", true), message);
            }
            else if (messageCmd == "start-round")
            {
              if (WebSocketMiddleware.LoadedQuizzes.ContainsKey(SiteId) && WebSocketMiddleware.LoadedQuizzes[SiteId].ContainsKey("min") && WebSocketMiddleware.LoadedQuizzes[SiteId]["min"].ContainsKey(GameId))
              {
                var gameData = WebSocketMiddleware.LoadedQuizzes[SiteId]["min"][GameId];
                if (gameData.ContainsKey("rounds"))
                {
                  foreach (var roundData in gameData["rounds"])
                  {
                    var roundObject = JObject.Parse(roundData.ToString());
                    if (roundObject.ContainsKey("roundid") && roundObject.Value<int>("roundid") == (int)messageData)
                    {
                      WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("round", roundObject), message);
                    }
                    else
                    {
                      continue;
                    }
                  }
                  return messageOutput;
                }
              }
              return WriteClientError("Game data not found");
            }
            else if (messageCmd == "end-round")
            {
              WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("question", null), message);
              WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("round", null), message);
            }
            else if (messageCmd == "end-question")
            {
              WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("question", null), message);
            }
            else if (messageCmd == "update-score-template")
            {
              try
              {
                var fullData = JObject.Parse(messageData.ToString());
                WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("overridescoring", fullData), message);
              }
              catch (Exception)
              {
                return WriteClientError("Score template corrupt, please refresh and try again.");
              }
            }
            else if (messageCmd == "send-question")
            {
              var fullData = new JObject();
              try {
                fullData = JObject.Parse(messageData.ToString());
              }
              catch (Exception)
              {
                return WriteClientError("Invalid question data, please try again.");
              }

              if (!fullData.ContainsKey("round") || !fullData.ContainsKey("question"))
              {
                return WriteClientError("Round / question not specified");
              }
              if (WebSocketMiddleware.LoadedQuizzes.ContainsKey(SiteId) && WebSocketMiddleware.LoadedQuizzes[SiteId].ContainsKey("min") && WebSocketMiddleware.LoadedQuizzes[SiteId]["min"].ContainsKey(GameId))
              {
                var gameData = WebSocketMiddleware.LoadedQuizzes[SiteId]["full"][GameId];
                var isLastQuestion = false;
                if (gameData.ContainsKey("rounds"))
                {
                  foreach (var roundData in gameData["rounds"])
                  {
                    var roundObject = JObject.Parse(roundData.ToString());
                    if (roundObject.ContainsKey("roundid") && roundObject.Value<int>("roundid") == (int)fullData["round"])
                    {
                      if (roundObject.ContainsKey("questions"))
                      {
                        foreach (var questionData in roundObject["questions"])
                        {
                          isLastQuestion = questionData == roundObject["questions"].Last;
                          var questionObject = JObject.Parse(questionData.ToString());
                          if (questionObject.Value<int>("questionid") == (int)fullData["question"])
                          {
                            var answerNode = JArray.Parse(questionObject["answers"].ToString());
                            var correctAnswerNode = new JArray();
                            try
                            {
                              correctAnswerNode = JArray.Parse(questionObject["correctanswers"].ToString());
                            }
                            catch (Exception)
                            {
                              correctAnswerNode = new JArray(new JValue(questionObject["correctanswers"].ToString()));
                            }

                            if (answerNode.Count <= 1 || answerNode.Count == correctAnswerNode.Count)
                            {
                              questionObject["answers"].Parent.Remove();
                              questionObject["answerimages"].Parent.Remove();
                            }
                            if (questionObject["questionimage"].ToString() == "")
                            {
                              questionObject["questionimage"].Parent.Remove();
                            }
                            questionObject["correctanswers"].Parent.Remove();

                            if (questionObject.ContainsKey("category"))
                            {
                              questionObject["category"].Parent.Remove();
                            }

                            if (!WebSocketMiddleware.CurrentGameData[SiteId][GameId].ContainsKey("seenquestions"))
                            {
                              WebSocketMiddleware.CurrentGameData[SiteId][GameId]["seenquestions"] = new JObject();
                            }
                            var roundid = roundObject.GetValue("roundid");
                            var questionid = questionObject.GetValue("questionid");
                            var newKey = roundid + "-" + questionid;

                            WebSocketMiddleware.CurrentGameData[SiteId][GameId]["seenquestions"][newKey] = true;

                            WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("seenquestions", WebSocketMiddleware.CurrentGameData[SiteId][GameId]["seenquestions"]));

                            if (WebSocketMiddleware.CurrentGameData[SiteId][GameId].ContainsKey("roundjuststarted"))
                            {
                              WebSocketMiddleware.CurrentGameData[SiteId][GameId]["roundjuststarted"].Parent.Remove();
                            }
                            WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("question", questionObject), message);

                            if (isLastQuestion)
                            {
                              WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("lastquestion", true));
                            }
                          }
                        }
                      }
                    }
                    else
                    {
                      continue;
                    }
                  }
                  return messageOutput;
                }
              }
              return WriteClientError("Game data not found");
            }
            else if (messageCmd == "kickall")
            {
            }
            else if (messageCmd == "timer")
            {
              if (WebSocketMiddleware.CurrentGameData.ContainsKey(SiteId) && WebSocketMiddleware.CurrentGameData[SiteId].ContainsKey(GameId))
              {
                if (WebSocketMiddleware.CurrentGameData[SiteId][GameId].ContainsKey("timerrunning") && WebSocketMiddleware.CurrentGameData[SiteId][GameId]["timerrunning"].Type != JTokenType.Null)
                {
                  return WriteClientError("Timer already running");
                }
                var fullData = new JObject();
                try
                {
                  fullData = JObject.Parse(messageData.ToString());
                }
                catch(Exception)
                {
                  return WriteClientError("Invalid timer properties, please refresh and try again.");
                }
                if (!fullData.ContainsKey("time") || !fullData.ContainsKey("action"))
                {
                  return WriteClientError("Invalid properties");
                }
                var seconds = fullData["time"].Value<int>();
                var afterAction = fullData["action"].Value<int>();

                WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("timerrunning", seconds), message);
                WebSocketMiddleware.InitialiseTimer(SiteId, GameId, message, seconds, afterAction).ConfigureAwait(false);
              }
            }
            else if (messageCmd == "cancel-timer")
            {
              if (WebSocketMiddleware.CurrentGameData.ContainsKey(SiteId) && WebSocketMiddleware.CurrentGameData[SiteId].ContainsKey(GameId))
              {
                if (WebSocketMiddleware.CurrentGameData[SiteId][GameId].ContainsKey("timerrunning") && WebSocketMiddleware.CurrentGameData[SiteId][GameId]["timerrunning"].Type != JTokenType.Null)
                {
                  WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("timerrunning", null), message);
                  WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("timercancelled", true), message);
                }
              }
            }
            else if (messageCmd == "get-answers")
            {
              var questionId = (long)0;
              var roundId = (long)0;

              var fullData = new JObject();
              try
              {
                fullData = JObject.Parse(messageData.ToString());
              }
              catch (Exception)
              {
                return WriteClientError("Invalid answer properties, please try again.");
              }
              if (fullData.ContainsKey("questionid"))
              {
                questionId = fullData["questionid"].Value<long>();
              }
              if (fullData.ContainsKey("roundid"))
              {
                roundId = fullData["roundid"].Value<long>();
              }

              var currentGameAnswers = JObject.Parse(WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"].ToString());
              var output = new JObject();
              foreach(var k in currentGameAnswers)
              {
                var userData = JObject.Parse(k.Value.ToString());
                var userId = k.Key;
                if(userData.ContainsKey(roundId.ToString()))
                {
                  if(userData[roundId.ToString()][questionId.ToString()] != null)
                  {
                    output.Add(new JProperty(userId.ToString(), new JObject(new JProperty(roundId.ToString(), new JObject(new JProperty(questionId.ToString(), userData[roundId.ToString()][questionId.ToString()]))))));
                  }
                }
              }
              messageOutput.TryAdd("sendToSelf", new JObject(new JProperty("gameupdate", new JObject(new JProperty("teamsanswered", output)))));
              return messageOutput;
            }
            else if (messageCmd == "update-answer-score")
            {
              var thisUserId = (long)0;
              var thisRoundId = (long)0;
              var thisQuestionId = (long)0;
              var thisUpdatedScore = (long)0;
              try
              {
                thisUserId = (long)messageUser["userid"];
                thisRoundId = (long)messageData["roundid"];
                thisQuestionId = (long)messageData["questionid"];
                thisUpdatedScore = (long)messageData["score"];
              }
              catch (Exception)
              {
                return WriteClientError("Invalid correction properties, please try again.");
              }

              var thisGameData = WebSocketMiddleware.LoadedQuizzes[SiteId]["full"][GameId];
              var currentGameData = WebSocketMiddleware.CurrentGameData[SiteId][GameId];

              //TODO: Check that this data exists:

              if (WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"][thisUserId.ToString()][thisRoundId.ToString()] == null)
              {
                WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"][thisUserId.ToString()][thisRoundId.ToString()] = new JObject();
              }

              if (WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"][thisUserId.ToString()][thisRoundId.ToString()][thisQuestionId.ToString()] == null)
              {
                WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"][thisUserId.ToString()][thisRoundId.ToString()][thisQuestionId.ToString()] = new JObject();
              }

              var oldData = JObject.Parse(WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"][thisUserId.ToString()][thisRoundId.ToString()][thisQuestionId.ToString()].ToString());
              var oldScore = long.Parse(oldData["awardedscore"].ToString());
              var theirAnswers = new JArray();
              if (oldData.ContainsKey("incorrectanswers"))
              {
                try
                {
                  theirAnswers = JArray.Parse(oldData["incorrectanswers"].ToString());
                }
                catch (Exception)
                {
                  theirAnswers = new JArray(new JValue(oldData["incorrectanswers"].ToString()));
                }
              }
              if(theirAnswers.Count <= 0 )
              {
                if (oldData.ContainsKey("correctanswers"))
                {
                  try
                  {
                    theirAnswers = JArray.Parse(oldData["correctanswers"].ToString());
                  }
                  catch (Exception)
                  {
                    theirAnswers = new JArray(new JValue(oldData["correctanswers"].ToString()));
                  }
                }
              }
              var newOutput = new JObject(new JProperty("speedscoreindex", 0), new JProperty("awardedscore", thisUpdatedScore), new JProperty("autoscorer", false), new JProperty("amended", true), new JProperty("userid", thisUserId), new JProperty("score", thisUpdatedScore), new JProperty("amendedscore", thisUpdatedScore), new JProperty("questionid", thisQuestionId), new JProperty("roundid", thisRoundId));

              if(thisUpdatedScore > 0)
              {
                newOutput.Add(new JProperty("correctanswers", theirAnswers));
                newOutput.Add(new JProperty("incorrectanswers", new JArray()));
              }
              else
              {
                newOutput.Add(new JProperty("correctanswers", new JArray()));
                newOutput.Add(new JProperty("incorrectanswers", theirAnswers));
              }

              WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][thisUserId.ToString()] = (WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][thisUserId.ToString()].Value<long>() - oldScore) + thisUpdatedScore;
              WebSocketMiddleware.CurrentGameData[SiteId][GameId]["allusers"][thisUserId.ToString()]["score"] = WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][thisUserId.ToString()];
              WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"][thisUserId.ToString()][thisRoundId.ToString()][thisQuestionId.ToString()] = newOutput;

              var outputObject = new JObject();
              outputObject.Add(new JProperty(thisUserId.ToString(), newOutput));
              outputObject.Add(new JProperty("all", new JObject(new JProperty("gameupdate", new JObject(new JProperty("userscore", new JObject(new JProperty("userid", thisUserId), new JProperty("score", WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][thisUserId.ToString()]))))))));
              outputObject.Add(new JProperty("quizmaster", new JObject(new JProperty("request", new JObject(new JProperty("callback", "mc_teamanswered_cb"))), new JProperty("gameupdate", new JObject(new JProperty("teamsanswered", new JObject(new JProperty(thisUserId.ToString(), new JObject(new JProperty(thisRoundId.ToString(), new JObject(new JProperty(thisQuestionId.ToString(), newOutput))))))))))));

              outputObject[thisUserId.ToString()]["type"] = "answer-submit";

              await WebSocketMiddleware.BroadcastToMultiple(WebSocketMiddleware.GameClients[SiteId][GameId], outputObject);
            }
          }
          else if (messageType == "chat-message")
          {
            if (IsMuted)
            {
              var muteStr = "You are muted";
              if(MuteReason != "")
              {
                muteStr += ": " + MuteReason;
              }
              return WriteClientError(muteStr);
            }

            if (!WebSocketMiddleware.ChatData.ContainsKey(SiteId))
            {
              WebSocketMiddleware.ChatData.TryAdd(SiteId, new ConcurrentDictionary<long, JArray>());
            }
            if (!WebSocketMiddleware.ChatData[SiteId].ContainsKey(GameId))
            {
              var chatData = new JArray();
              WebSocketMiddleware.ChatData[SiteId].TryAdd(GameId, chatData);

              if (WebSocketMiddleware.CurrentGameData.ContainsKey(SiteId)) {
                if (WebSocketMiddleware.CurrentGameData[SiteId].ContainsKey(GameId))
                {
                  if (WebSocketMiddleware.CurrentGameData[SiteId][GameId].ContainsKey("chatbox"))
                  {
                    WebSocketMiddleware.ChatData[SiteId][GameId] = JArray.Parse(WebSocketMiddleware.CurrentGameData[SiteId][GameId]["chatbox"].ToString());
                  }
                }
              }
            }

            var chatId = WebSocketMiddleware.ChatData[SiteId][GameId].Count + 1;

            var chatObj = new JObject(new JProperty("chatid", chatId), new JProperty("userid", UserId), new JProperty("time", DateTime.Now), new JProperty("message", messageData));

            WebSocketMiddleware.ChatData[SiteId][GameId].Add(chatObj);
            messageOutput.TryAdd("sendToAll", new JObject(new JProperty("type", "chat-message"), new JProperty("data", chatObj)));

            WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("chatbox", WebSocketMiddleware.ChatData[SiteId][GameId]), new JObject(), false);
          }
          else if (messageType == "ping")
          {
            messageOutput.TryAdd("sendToSelf", message);
          }
          else if (messageType == "answer-submit")
          {
            if (IsQuizMaster || IsAdmin)
              return WriteClientError("Permission denied");

            var questionId = (long)0;
            var roundId = (long)0;
            var answer = new JArray();
            var fullData = new JObject();
            try
            {
              fullData = JObject.Parse(messageData.ToString());
            }
            catch(Exception)
            {
              return WriteClientError("Invalid answer properties, please try again.");
            }
            if (fullData.ContainsKey("questionid"))
            {
              questionId = fullData["questionid"].Value<long>();
            }
            if (fullData.ContainsKey("roundid"))
            {
              roundId = fullData["roundid"].Value<long>();
            }
            if (fullData.ContainsKey("answer"))
            {
              var answerStr = fullData["answer"].Value<string>();
              if (answerStr == "" || answerStr == null)
              {
                return WriteClientError("Invalid answer");
              }
              try
              {
                answer = JArray.Parse(answerStr);
              }
              catch (Exception)
              {
                answer = new JArray(new JValue(answerStr));
              }
            }

            if (WebSocketMiddleware.AnsweredQuestions.ContainsKey(SiteId) &&
                WebSocketMiddleware.AnsweredQuestions[SiteId].ContainsKey(GameId) &&
                WebSocketMiddleware.AnsweredQuestions[SiteId][GameId].ContainsKey(UserId) &&
                WebSocketMiddleware.AnsweredQuestions[SiteId][GameId][UserId].ContainsKey(roundId) &&
                WebSocketMiddleware.AnsweredQuestions[SiteId][GameId][UserId][roundId].ContainsKey(questionId))
            {
              return WriteClientError("Question already answered.");
            }

            var currentGameData = WebSocketMiddleware.CurrentGameData[SiteId][GameId];

            var checkAlwaysAvailableRounds = false;
            if (!currentGameData.ContainsKey("round") || currentGameData["round"].Type == JTokenType.Null)
            {
              checkAlwaysAvailableRounds = true;
            }
            if (!checkAlwaysAvailableRounds && currentGameData["round"]["roundid"].Value<long>() != roundId)
            {
              checkAlwaysAvailableRounds = true;
            }
            if (!checkAlwaysAvailableRounds && (!currentGameData.ContainsKey("question") || currentGameData["question"].Type == JTokenType.Null))
            {
              checkAlwaysAvailableRounds = true;
            }
            if (!checkAlwaysAvailableRounds && currentGameData["question"]["questionid"].Value<long>() != questionId)
            {
              checkAlwaysAvailableRounds = true;
            }

            var thisGameData = WebSocketMiddleware.LoadedQuizzes[SiteId]["full"][GameId];
            var alwaysAvailableAccepted = false;
            var modObj = new JObject();
            if (thisGameData.ContainsKey("roundmodifiers"))
            {
              var roundModRaw = thisGameData["roundmodifiers"].Value<string>();
              roundModRaw = (roundModRaw == "" ? "{}" : roundModRaw);
              modObj = JObject.Parse(roundModRaw);
              if (modObj.ContainsKey(roundId.ToString()))
              {
                if (modObj[roundId.ToString()]["round-mod-always-available"].Value<string>().ToLower() == "true")
                {
                  alwaysAvailableAccepted = true;
                }
              }
            }

            var defaultModRaw = "{}";
            if (modObj.ContainsKey("default"))
            {
              defaultModRaw = modObj["default"].ToString();
            }

            var thisModObjRaw = defaultModRaw;
            if (modObj.ContainsKey(roundId.ToString()))
            {
              thisModObjRaw = modObj[roundId.ToString()].ToString();
            }

            if (currentGameData.ContainsKey("overridescoring"))
            {
              var overrideRaw = currentGameData["overridescoring"].ToString();
              overrideRaw = (overrideRaw == "" ? "{}" : overrideRaw);
              thisModObjRaw = overrideRaw;
            }

            var thisModObj = JObject.Parse(thisModObjRaw);

            if (checkAlwaysAvailableRounds && !alwaysAvailableAccepted)
            {
              return WriteClientError("Too late");
            }
            var allRoundsData = thisGameData["rounds"];
            var thisRoundObj = new JObject();
            var foundRound = false;
            foreach (var k in allRoundsData)
            {
              thisRoundObj = JObject.Parse(k.ToString());
              if (thisRoundObj["roundid"].Value<long>() == roundId)
              {
                foundRound = true;
                break;
              }
            }
            var questionData = new JObject();
            var foundQuestion = false;
            foreach (var k in thisRoundObj["questions"])
            {
              questionData = JObject.Parse(k.ToString());
              if (questionData["questionid"].Value<long>() == questionId)
              {
                foundQuestion = true;
                break;
              }
            }

            if (!foundRound)
            {
              return WriteClientError("Invalid round");
            }

            if (!foundQuestion)
            {
              return WriteClientError("Invalid question");
            }

            if (!WebSocketMiddleware.AnsweredQuestions[SiteId][GameId].ContainsKey(UserId))
            {
              WebSocketMiddleware.AnsweredQuestions[SiteId][GameId].TryAdd(UserId, new ConcurrentDictionary<long, ConcurrentDictionary<long, bool>>());
            }
            if (!WebSocketMiddleware.AnsweredQuestions[SiteId][GameId][UserId].ContainsKey(roundId))
            {
              WebSocketMiddleware.AnsweredQuestions[SiteId][GameId][UserId].TryAdd(roundId, new ConcurrentDictionary<long, bool>());
            }

            WebSocketMiddleware.AnsweredQuestions[SiteId][GameId][UserId][roundId].TryAdd(questionId, true);


            if (WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()] == null)
            {
              WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()] = 0;
            }

            MarkAnswer(roundId, questionId, questionData, thisModObj, answer);

            var leaderboardData = new JObject();
            if (WebSocketMiddleware.CurrentGameData[SiteId][GameId].ContainsKey("leaderboard"))
            {
              leaderboardData = JObject.Parse(WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"].ToString());
            }
            Score = (long)(leaderboardData.ContainsKey(UserId.ToString()) ? leaderboardData[UserId.ToString()] : 0);
            var outputMsg = new JObject(new JProperty("type", "answer-submit"), new JProperty("roundid", roundId), new JProperty("questionid", questionId), new JProperty("userid", UserId), new JProperty("score", Score), new JProperty("awardedscore", LastScoreAwarded), new JProperty("incorrectanswers", LastScoresIncorrect), new JProperty("correctanswers", LastScoresCorrect), new JProperty("autoscorer", !(!thisModObj.ContainsKey("round-mod-auto-scorer") || thisModObj["round-mod-auto-scorer"].Value<Boolean>() == false)));

            var acceptedAnswers = JObject.Parse(currentGameData.ContainsKey("teamsanswered") ? currentGameData["teamsanswered"].ToString() : "{}");
            var strUserId = UserId.ToString();
            var strRoundId = roundId.ToString();
            if (!acceptedAnswers.ContainsKey(strUserId))
            {
              acceptedAnswers.Add(new JProperty(strUserId, new JObject()));
            }
            if (acceptedAnswers[strUserId][strRoundId] == null)
            {
              acceptedAnswers[strUserId][strRoundId] = new JObject();
            }

            messageOutput.TryAdd("sendToSelf", outputMsg);

            var thisRoundMod = JObject.Parse(modObj.ContainsKey(strRoundId) ? modObj[strRoundId].ToString() : "{}");

            if (thisRoundMod.ContainsKey("round-mod-end-after"))
            {
              var confirmRoundEnd = false;
              var endRoundMode = thisRoundMod.Value<string>("round-mod-end-after");

              if (endRoundMode == "auto")
              {
                var shouldEndRound = false;
                var roundObj = JArray.Parse(WebSocketMiddleware.LoadedQuizzes[SiteId]["full"][GameId]["rounds"].ToString());

                for (var i = 0; i < roundObj.Count; i++)
                {
                  if (roundObj[i].Value<string>("roundid") == strRoundId)
                  {
                    var questions = JArray.Parse(roundObj[i]["questions"].ToString());
                    for (var a = 0; a < questions.Count; a++)
                    {
                      if (questions[a].Value<long>("questionid") == questionId)
                      {
                        if (questions[a] == questions.Last)
                        {
                          shouldEndRound = true;
                        }
                      }
                    }
                  }
                }

                if (shouldEndRound)
                {
                  var allTeamCount = JObject.Parse(WebSocketMiddleware.CurrentGameData[SiteId][GameId]["connectedusers"].ToString()).Count;
                  var acceptedAnswerCount = 0;

                  foreach (var k in acceptedAnswers)
                  {
                    var rounds = JObject.Parse(k.Value.ToString());
                    if (rounds.ContainsKey(strRoundId))
                    {
                      var round = JObject.Parse(rounds[strRoundId].ToString());
                      if (round.ContainsKey(questionId.ToString()))
                      {
                        acceptedAnswerCount++;
                      }
                    }
                  }

                  if (acceptedAnswerCount == allTeamCount)
                  {
                    confirmRoundEnd = true;
                  }
                }
              }
              else if (endRoundMode == "after" && thisRoundMod.ContainsKey("round-mod-endroundafter-value"))
              {
                var totalPointsCount = (long)0;
                var pointsToEndRound = long.Parse(thisRoundMod["round-mod-endroundafter-value"].ToString());
                foreach (var k in acceptedAnswers)
                {
                  var rounds = JObject.Parse(k.Value.ToString());
                  if (rounds.ContainsKey(strRoundId))
                  {
                    var round = JObject.Parse(rounds[strRoundId].ToString());
                    if (round.ContainsKey(questionId.ToString()))
                    {
                      totalPointsCount += long.Parse(round[questionId].ToString());
                      if (totalPointsCount >= pointsToEndRound)
                      {
                        confirmRoundEnd = true;
                        break;
                      }
                    }
                  }
                }
              }

              if (confirmRoundEnd)
              {
                WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("question", null), message);
                WebSocketMiddleware.UpdateCurrentGameData(SiteId, GameId, new JProperty("round", null), message);
              }
            }
          }
          else if (messageType == "site-change")
          {
            if (messageSiteId == null)
              return WriteClientError("Site identifiers missing");

            var isThisSite = (long)messageUser["siteId"] == SiteId;

            if ((isThisSite && !IsAdmin || !IsQuizMaster) || (!isThisSite && !IsAdmin))
              return WriteClientError("Permission denied");

            if (messageCmd == "disable-site" && IsAdmin)
            {
              //WebSocketMiddleware.SiteData
            }

            messageOutput.TryAdd("sendToAll", new JObject(new JProperty("type", "update-site"), new JProperty("update", messageData)));
          }
          else if (messageType == "change-user")
          {
            if (messageUser["userid"].ToString() == "" || messageUser["siteid"].ToString() == "")
              return WriteClientError("User identifiers missing");

            var thisUserId = (long)messageUser["userid"];
            var thisSiteId = (long)messageUser["siteid"];

            var isThisUser = thisUserId == UserId;
            var isThisSite = thisSiteId == SiteId;

            var updatedClientData = new JObject();

            if (!isThisUser && !isThisSite && !IsAdmin)
              return WriteClientError("Permission denied");

            if (!isThisUser && isThisSite && !IsQuizMaster)
              return WriteClientError("Permission denied");

            if (messageUser["username"] == null && messageUser["avatar"] == null && messageUser["muted"] == null)
              return WriteClientError("User data not specified");

            foreach(var k in WebSocketMiddleware.AllClients)
            {
              var user = k.Value;
              if(user.SiteId == thisSiteId && user.UserId == thisUserId)
              {
                if(messageUser["username"] != null)
                {
                  user.TeamName = messageUser["username"].ToString();
                }
                if (messageUser["avatar"] != null)
                {
                  user.AvatarURL = messageUser["avatar"].ToString();
                }
                if (messageUser["muted"] != null)
                {
                  user.IsMuted = messageUser["muted"].Value<bool>();
                  if (user.IsMuted && messageUser["mutereason"] != null) {
                    user.MuteReason = messageUser["mutereason"].ToString();
                  }
                }
                if (messageUser["banned"] != null)
                {
                  user.IsBanned = messageUser["banned"].Value<bool>();
                  if (user.IsBanned && messageUser["banreason"] != null)
                  {
                    user.BanReason = messageUser["banreason"].ToString();
                  }
                }
              }
            }
            foreach (var k in WebSocketMiddleware.GameClients[SiteId][GameId])
            {
              var user = k.Value;
              if (user.UserId == thisUserId)
              {
                if (messageUser["username"] != null)
                {
                  user.TeamName = messageUser["username"].ToString();
                }
                if (messageUser["avatar"] != null)
                {
                  user.AvatarURL = messageUser["avatar"].ToString();
                }
                if (messageUser["muted"] != null)
                {
                  user.IsMuted = messageUser["muted"].Value<bool>();
                  if (user.IsMuted && messageUser["mutereason"] != null)
                  {
                    user.MuteReason = messageUser["mutereason"].ToString();
                  }
                }
                if (messageUser["banned"] != null)
                {
                  user.IsBanned = messageUser["banned"].Value<bool>();
                  if (user.IsBanned && messageUser["banreason"] != null)
                  {
                    user.BanReason = messageUser["banreason"].ToString();
                  }
                }
              }
            }
            foreach (var k in WebSocketMiddleware.CurrentGameData[SiteId][GameId])
            {
              if(k.Key == "allusers" || k.Key == "connectedusers")
              {
                var userData = k.Value;
                if(userData[thisUserId.ToString()] != null)
                {
                  if (messageUser["username"] != null)
                  {
                    userData[thisUserId.ToString()]["username"] = messageUser["username"].ToString();
                  }
                  if (messageUser["avatar"] != null)
                  {
                    userData[thisUserId.ToString()]["avatarurl"] = messageUser["avatar"].ToString();
                  }
                  if (messageUser["muted"] != null)
                  {
                    userData[thisUserId.ToString()]["muted"] = messageUser["muted"].Value<bool>();
                  }
                  if (messageUser["banned"] != null)
                  {
                    userData[thisUserId.ToString()]["banned"] = messageUser["banned"].Value<bool>();
                  }
                  WebSocketMiddleware.CurrentGameData[SiteId][GameId][k.Key][thisUserId.ToString()] = userData[thisUserId.ToString()];
                  updatedClientData = JObject.Parse(userData[thisUserId.ToString()].ToString());
                }
              }
            }

            AES256 aes = new AES256();

            var dbClient = new MongoClient(Program.MONGO_DB_SVR);
            var database = dbClient.GetDatabase(Program.MONGO_DB_DBN);
            var collection = database.GetCollection<BsonDocument>("user_sessions");

            if (messageUser["username"] != null)
            {
              await collection.UpdateManyAsync(
                     filter: BsonDocument.Parse(new JObject(new JProperty("siteid", thisSiteId), new JProperty("userid", thisUserId)).ToString()),
                     Builders<BsonDocument>.Update.Set("teamname", aes.Encrypt(messageUser["username"].ToString(), "HTRQUIZ").ToString())
              );
            }
            else if (messageUser["avatar"] != null)
            {
              await collection.UpdateManyAsync(
                     filter: BsonDocument.Parse(new JObject(new JProperty("siteid", thisSiteId), new JProperty("userid", thisUserId)).ToString()),
                     Builders<BsonDocument>.Update.Set("avatar", aes.Encrypt(messageUser["avatar"].ToString(), "HTRQUIZ").ToString())
              );
            }
            else if (messageUser["muted"] != null)
            {
              var muteReason = "";
              if(messageUser["mutereason"] != null)
              {
                muteReason = messageUser["mutereason"].ToString();
              }
              await collection.UpdateManyAsync(
                     filter: BsonDocument.Parse(new JObject(new JProperty("siteid", thisSiteId), new JProperty("userid", thisUserId)).ToString()),
                     Builders<BsonDocument>.Update.Set("ismuted", messageUser["muted"].Value<bool>())
                                                  .Set("mutereason", aes.Encrypt(muteReason, "HTRQUIZ").ToString())
              );
            }
            else if (messageUser["banned"] != null)
            {
              var banReason = "";
              if (messageUser["banreason"] != null)
              {
                banReason = messageUser["banreason"].ToString();
              }
              await collection.UpdateManyAsync(
                     filter: BsonDocument.Parse(new JObject(new JProperty("siteid", thisSiteId), new JProperty("userid", thisUserId)).ToString()),
                     Builders<BsonDocument>.Update.Set("isbanned", messageUser["banned"].Value<bool>())
                                                  .Set("banreason", aes.Encrypt(banReason, "HTRQUIZ").ToString())
              );
            }

            messageOutput.TryAdd("sendToAll", new JObject(new JProperty("type", "user-change"), new JProperty("user", updatedClientData)));

            return messageOutput;
          }
          else if (messageType == "delete-message")
          {
            if (!IsQuizMaster && !IsAdmin)
              return WriteClientError("Permission denied");

            var fullDataObj = new JObject();
            try
            {
              fullDataObj = JObject.Parse(messageData.ToString());
            }
            catch(Exception)
            {
              return WriteClientError("Invalid delete properties, please try again");
            }
            
            if (!(fullDataObj.ContainsKey("msgid") && fullDataObj.ContainsKey("userid")))
              return WriteClientError("Invalid identifiers");

            if (WebSocketMiddleware.CurrentGameData[SiteId][GameId].ContainsKey("chatbox"))
            {
              foreach (var k in WebSocketMiddleware.CurrentGameData[SiteId][GameId]["chatbox"])
              {
                var chatObj = JObject.Parse(k.ToString());
                if (chatObj["userid"].ToString() == fullDataObj["userid"].ToString() && chatObj["chatid"].ToString() == fullDataObj["msgid"].ToString())
                {
                  k.Remove();
                  messageOutput.TryAdd("sendToAll", new JObject(new JProperty("type", "chat-remove"), new JProperty("chatid", fullDataObj.Value<long>("msgid")), new JProperty("userid", fullDataObj.Value<long>("userid"))));
                  break;
                }
              }
            }
          }
        }
        return messageOutput;
      }catch (Exception ex)
      {
        Console.WriteLine($"Socket {SocketId}:");
        Console.WriteLine(ex.StackTrace.ToString());
        Program.ReportException(ex);

        return new ConcurrentDictionary<string, JObject>();
      }
    }

    public void JoinGame(long localGameId)
    {
      GameId = localGameId;
    }

    public int SocketId { get; private set; }

    public string SessionId { get; private set; }

    public WebSocket Socket { get; private set; }

    public bool IsAuthed { get; set; }

    public bool IsAdmin { get; private set; }

    public bool IsQuizMaster { get; private set; }

    public bool IsMuted { get; set; }

    public string MuteReason { get; set; }

    public bool IsBanned { get; set; }

    public string BanReason { get; set; }

    public long UserId { get; private set; }

    public string TeamName { get; private set; }
    
    public long Score { get; private set; }    

    public long LastScoreAwarded { get; set; }

    public JArray LastScoresCorrect { get; set; }

    public JArray LastScoresIncorrect { get; set; }

    public long SiteId { get; private set; }

    public long GameId { get; private set; }

    public string AvatarURL { get; private set; }

    public TaskCompletionSource<object> TaskCompletion { get; private set; }

    public Channel<string> BroadcastQueue { get; private set; }

    public CancellationTokenSource BroadcastLoopTokenSource { get; set; } = new CancellationTokenSource();

    public async Task BroadcastLoopAsync()
    {
      var cancellationToken = BroadcastLoopTokenSource.Token;
      while (!cancellationToken.IsCancellationRequested)
      {
        try
        {
          while (await BroadcastQueue.Reader.WaitToReadAsync(cancellationToken))
          {
            string message = await BroadcastQueue.Reader.ReadAsync();
            Console.WriteLine($"Socket {SocketId}: Sending from queue.");
            var msgbuf = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message));
            await Socket.SendAsync(msgbuf, WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);
          }
        }
        catch (WebSocketException ex)
        {
          Console.WriteLine($"Socket {SocketId}: Websocket exception - " + ex.Message);
        }
        catch (OperationCanceledException)
        {
          // normal upon task/token cancellation, disregard
        }
        catch (Exception ex)
        {
          Program.ReportException(ex);
        }
      }
    }

    public void MarkAnswer(long RoundId, long QuestionId, JObject questionData, JObject roundScoring, JArray answers)
    {
      var answerCorrect = new ConcurrentDictionary<string, bool>();
      var answerIncorrect = new ConcurrentDictionary<string, bool>();
      var incorrectScoreAmt = (long)0;

      if (roundScoring.ContainsKey("round-mod-score-incorrect"))
      {
        incorrectScoreAmt = long.Parse(roundScoring["round-mod-score-incorrect"].ToString());
      }


      LastScoresCorrect = new JArray();
      LastScoresIncorrect = new JArray();
      if (questionData.ContainsKey("correctanswers"))
      {
        var correctAnswersObj = new JArray();
        try
        {
          correctAnswersObj = JArray.Parse(questionData["correctanswers"].ToString());
        }
        catch (Exception)
        {
          correctAnswersObj = new JArray(new JValue(questionData["correctanswers"].ToString()));
        }
        foreach (var a in answers)
        {
          foreach (var k in correctAnswersObj)
          {
            var compValue = (double)0.7;
            if (correctAnswersObj.Count < questionData["answers"].Count())
            {
              compValue = (double)1;
            }

            var answer = a.Value<string>();
            if (answer == null || answer == "null")
            {
              continue;
            }
            answer = answer.ToLower().Trim();
            double comp = StringCompare.CalculateSimilarity(k.Value<string>().ToLower().Trim(), answer);
            if (comp >= compValue)
            {
              LastScoresCorrect.Add(a.Value<string>());
              answerCorrect[answer] = true;
            }
            else
            {
              answerIncorrect[answer] = true;
            }
          }
        }
      }

      if (!roundScoring.ContainsKey("round-mod-auto-scorer") || roundScoring["round-mod-auto-scorer"].Value<Boolean>() == false)
      {
        answerCorrect = new ConcurrentDictionary<string, bool>();
        answerIncorrect = new ConcurrentDictionary<string, bool>();
      }
      foreach (var markedincorrect in answerIncorrect)
      {
        if (answerCorrect.ContainsKey(markedincorrect.Key))
        {
          answerIncorrect.TryRemove(markedincorrect.Key, out _);
        }
      }

      foreach (var k in answerIncorrect)
      {
        LastScoresIncorrect.Add(k.Key);
      }

      var incScore = (long)0;

      if (!WebSocketMiddleware.QuestionAnswerCount[SiteId][GameId].ContainsKey(RoundId))
      {
        WebSocketMiddleware.QuestionAnswerCount[SiteId][GameId].TryAdd(RoundId, new ConcurrentDictionary<long, long>());
      }


      var speedScoreAdded = false;
      var correctAnswerScore = (long)(roundScoring.ContainsKey("round-mod-score-correct") ? roundScoring["round-mod-score-correct"].Value<long>() : 0);
      var speedScoreUsed = 0;

      if (answerCorrect.Count > 0)
      {
        foreach (var answer in answerCorrect)
        {
          if (!speedScoreAdded)
          {
            var speedScoreIndex = 1;
            if (WebSocketMiddleware.QuestionAnswerCount[SiteId][GameId][RoundId].ContainsKey(QuestionId))
            {
              speedScoreIndex = (int)WebSocketMiddleware.QuestionAnswerCount[SiteId][GameId][RoundId][QuestionId] + 1;
            }

            WebSocketMiddleware.QuestionAnswerCount[SiteId][GameId][RoundId][QuestionId] = speedScoreIndex;

            if (roundScoring.ContainsKey("round-mod-speed-score-" + speedScoreIndex.ToString()))
            {
              incScore += roundScoring["round-mod-speed-score-" + speedScoreIndex.ToString()].Value<long>();
              speedScoreUsed = speedScoreIndex;
            }
            else
            {
              incScore += correctAnswerScore;
            }
            speedScoreAdded = true;
          }
          else
          {
            incScore += correctAnswerScore;
          }
        }
      }
      if (answerIncorrect.Count > 0)
      {
        foreach (var ans in answerIncorrect)
        {
          incScore += incorrectScoreAmt;
        }
      }

      LastScoreAwarded = incScore;

      WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()] = WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()].Value<long>() + LastScoreAwarded;

      if (WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()] == null)
      {
        WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()] = new JObject();
      }

      WebSocketMiddleware.CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()][QuestionId.ToString()] = new JObject(new JProperty("speedscoreindex", speedScoreUsed), new JProperty("awardedscore", LastScoreAwarded), new JProperty("correctanswers", LastScoresCorrect), new JProperty("incorrectanswers", LastScoresIncorrect), new JProperty("autoscorer", !(!roundScoring.ContainsKey("round-mod-auto-scorer") || roundScoring["round-mod-auto-scorer"].Value<Boolean>() == false)));

      WebSocketMiddleware.CurrentGameData[SiteId][GameId]["allusers"][UserId.ToString()]["score"] = WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()];

      var outputMsg = new JObject();
      outputMsg.Add(new JProperty("all", new JObject(new JProperty("gameupdate", new JObject(new JProperty("userscore", new JObject(new JProperty("userid", UserId), new JProperty("score", WebSocketMiddleware.CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()]))))))));
      outputMsg.Add(new JProperty("quizmaster", new JObject(new JProperty("request", new JObject(new JProperty("callback", "mc_teamanswered_cb"))), new JProperty("gameupdate", new JObject(new JProperty("teamsanswered", new JObject(new JProperty(UserId.ToString(), new JObject(new JProperty(RoundId.ToString(), new JObject(new JProperty(QuestionId.ToString(), new JObject(new JProperty("speedscoreindex", 0), new JProperty("awardedscore", LastScoreAwarded), new JProperty("correctanswers", LastScoresCorrect), new JProperty("incorrectanswers", LastScoresIncorrect), new JProperty("autoscorer", !(!roundScoring.ContainsKey("round-mod-auto-scorer") || roundScoring["round-mod-auto-scorer"].Value<Boolean>() == false)))))))))))))));

      WebSocketMiddleware.BroadcastToMultiple(WebSocketMiddleware.GameClients[SiteId][GameId], outputMsg);
    }
  }
}

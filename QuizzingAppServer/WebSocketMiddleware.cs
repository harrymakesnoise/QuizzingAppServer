using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Net;

namespace QuizzingAppServer
{
  public class WebSocketMiddleware : IMiddleware
  {
    private static int SocketCounter = 0;

    // The key is a socket id
    public static ConcurrentDictionary<string, ConnectedClient> AllClients = new ConcurrentDictionary<string, ConnectedClient>();

    // SiteID -> GameID -> UserID -> ClientObj
    public static ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, ConnectedClient>>> GameClients = new ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, ConnectedClient>>>();

    // SiteID -> Full / Min -> GameID -> GameObj
    public static ConcurrentDictionary<long, ConcurrentDictionary<string, ConcurrentDictionary<long, JObject>>> LoadedQuizzes = new ConcurrentDictionary<long, ConcurrentDictionary<string, ConcurrentDictionary<long, JObject>>>();

    // SiteID -> GameID -> CurrentObj
    public static ConcurrentDictionary<long, ConcurrentDictionary<long, JObject>> CurrentGameData = new ConcurrentDictionary<long, ConcurrentDictionary<long, JObject>>();

    // SiteID -> GameID -> RoundID -> QuestionID -> Count
    public static ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, long>>>> QuestionAnswerCount = new ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, long>>>>();

    // SiteID -> GameID -> RoundID -> QuestionID -> UserID -> bool
    public static ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, bool>>>>> AnsweredQuestions = new ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, ConcurrentDictionary<long, bool>>>>>();

    // SiteID -> GameID -> ChatData
    public static ConcurrentDictionary<long, ConcurrentDictionary<long, JArray>> ChatData = new ConcurrentDictionary<long, ConcurrentDictionary<long, JArray>>();

    // JArray of database commands
    public static JArray dbActions = new JArray();

    public static CancellationTokenSource SocketLoopTokenSource = new CancellationTokenSource();

    private static bool ServerIsRunning = true;

    private static DateTime GCLastTime = new DateTime();

    private static CancellationTokenRegistration AppShutdownHandler;

    // use dependency injection to grab a reference to the hosting container's lifetime cancellation tokens
    public WebSocketMiddleware(IHostApplicationLifetime hostLifetime)
    {
      // gracefully close all websockets during shutdown (only register on first instantiation)
      if (AppShutdownHandler.Token.Equals(CancellationToken.None))
        AppShutdownHandler = hostLifetime.ApplicationStopping.Register(ApplicationShutdownHandler);
    }

    public async Task InvokeAsync(HttpContext context, RequestDelegate next)
    {
      try
      {
        if (ServerIsRunning)
        {
          if (context.WebSockets.IsWebSocketRequest)
          {
            int socketId = Interlocked.Increment(ref SocketCounter);
            var socket = await context.WebSockets.AcceptWebSocketAsync();
            var completion = new TaskCompletionSource<object>();

            var client = new ConnectedClient(socketId, socket, completion);

            AllClients.TryAdd(socketId.ToString(), client);
            Console.WriteLine($"Socket {socketId}: New connection.");

            // TaskCompletionSource<> is used to keep the middleware pipeline alive;
            // SocketProcessingLoop calls TrySetResult upon socket termination
            _ = Task.Run(() => SocketProcessingLoopAsync(client).ConfigureAwait(false));
            await completion.Task;
          }
          else
          {
            if (context.Request.Headers["Accept"][0].Contains("text/html"))
            {
              // ignore other requests (such as favicon)
              // potentially other middleware will handle it (see finally block)
            }
          }
        }
        else
        {
          // ServerIsRunning = false
          // HTTP 409 Conflict (with server's current state)
          context.Response.StatusCode = 409;
        }
      }
      catch (Exception ex)
      {
        // HTTP 500 Internal server error
        context.Response.StatusCode = 500;
        Program.ReportException(ex);
      }
      finally
      {
        // if this middleware didn't handle the request, pass it on
        if (!context.Response.HasStarted)
          await next(context);
      }
    }

    public static async Task BroadcastToMultiple(ConcurrentDictionary<long, ConnectedClient> GameClients, JObject data)
    {
      if (data.Count > 0)
      {
        foreach (var k in data)
        {
          var id = (long)0;
          if (long.TryParse(k.Key, out id))
          {
            if (GameClients.ContainsKey(id))
            {
              GameClients[id].BroadcastQueue.Writer.TryWrite(k.Value.ToString());
            }
          }
          if(id == null || id == 0)
          {
            foreach (var kvp in GameClients)
            {
              var shouldSend = false;
              if (kvp.Value.IsAuthed)
              {
                if (k.Key == "quizmaster")
                {
                  if (kvp.Value.IsAdmin || kvp.Value.IsQuizMaster)
                  {
                    shouldSend = true;
                  }
                }
                else if (k.Key == "players")
                {
                  if (!(kvp.Value.IsQuizMaster && kvp.Value.IsAdmin))
                  {
                    shouldSend = true;
                  }
                }
                else if(k.Key == "all")
                {
                  shouldSend = true;
                }
                else
                {
                  throw new ArgumentException("Unknown recipient: " + k.Key);
                }

                if(shouldSend)
                {
                  kvp.Value.BroadcastQueue.Writer.TryWrite(k.Value.ToString());
                }
              }
            }
          }
        }
      }
    }

    public static async Task BroadcastToAll(ConcurrentDictionary<long, ConnectedClient> GameClients, string message, string messageRecipients = "", long UserId = 0)
    {
      foreach (var kvp in GameClients)
      {
        var shouldSend = false;
        if (kvp.Value.IsAuthed)
        {
          if (UserId > 0 && kvp.Value.UserId == UserId)
          {
            shouldSend = true;
          } 
          else if (messageRecipients == "")
          {
            shouldSend = true;
          }
          else if (messageRecipients == "quizmaster")
          {
            if (kvp.Value.IsQuizMaster)
            {
              shouldSend = true;
            }
          }
          else if (messageRecipients == "players")
          {
            if (!kvp.Value.IsQuizMaster)
            {
              shouldSend = true;
            }
          }
        }
        if (shouldSend)
        {
          kvp.Value.BroadcastQueue.Writer.TryWrite(message);
        }
      }
      return;
    }

    // event-handlers are the sole case where async void is valid
    public static async void ApplicationShutdownHandler()
    {
      ServerIsRunning = false;
      await CloseAllSocketsAsync();
    }

    private static void StoreQuizPacket(ConnectedClient client, String BroadcastList, JObject inputMessage, JObject outputMessage)
    {
      var nowDt = DateTime.Now;
      var DateTimeStr = nowDt.ToString("yyyyMMddTHH:mm:ssZ");

      var data = new JObject(new JProperty("siteid", client.SiteId),
                          new JProperty("userid", client.UserId),
                          new JProperty("datetime", DateTimeStr),
                          new JProperty("broadcast", BroadcastList),
                          new JProperty("input", inputMessage),
                          new JProperty("output", outputMessage));

      var packet = new JObject(
                      new JProperty("collectionname", "quiz_packets"),
                      new JProperty("action", "insert"),
                      new JProperty("data", data)
                   );

      dbActions.Insert(0, packet);
    }

    public static void RemoveClientFromGame(long SiteId, long GameId, ConnectedClient client)
    {
      if (GameClients.ContainsKey(client.SiteId) && GameClients[client.SiteId].ContainsKey(client.GameId) && GameClients[client.SiteId][client.GameId].ContainsKey(client.UserId))
      {
        if (CurrentGameData.ContainsKey(client.SiteId) && CurrentGameData[client.SiteId].ContainsKey(client.GameId))
        {
          if (!(client.IsQuizMaster || client.IsAdmin))
          {
            if (CurrentGameData[client.SiteId][client.GameId].ContainsKey("connectedusers"))
            {
              if(CurrentGameData[client.SiteId][client.GameId]["connectedusers"][client.UserId.ToString()] != null)
              {
                CurrentGameData[client.SiteId][client.GameId]["connectedusers"][client.UserId.ToString()].Parent.Remove();
              }
              UpdateCurrentGameData(client.SiteId, client.GameId, new JProperty("connectedusers", CurrentGameData[client.SiteId][client.GameId]["connectedusers"]));
              
              BroadcastToAll(GameClients[SiteId][GameId], "{\"type\": \"remove-player\", \"userid\": " + client.UserId + "}");
            }
          }

          if (CurrentGameData[client.SiteId][client.GameId].ContainsKey("allusers"))
          {
            if (CurrentGameData[client.SiteId][client.GameId]["allusers"][client.UserId.ToString()] != null)
            {
              CurrentGameData[client.SiteId][client.GameId]["allusers"][client.UserId.ToString()]["online"] = false;
              UpdateCurrentGameData(SiteId, GameId, new JProperty("allusers", CurrentGameData[client.SiteId][client.GameId]["allusers"]));
            }
          }
        }
        GameClients[client.SiteId][client.GameId].TryRemove(client.UserId, out _);
      }
      AllClients.TryRemove(client.SocketId.ToString(), out _);
    }

    public static async Task LoadCurrentGameData(long SiteId, long GameId)
    {
      if (!CurrentGameData.ContainsKey(SiteId))
      {
        CurrentGameData.TryAdd(SiteId, new ConcurrentDictionary<long, JObject>());
      }
      if (!CurrentGameData[SiteId].ContainsKey(GameId))
      {
        var dbClient = new MongoClient(Program.MONGO_DB_SVR);
        var database = dbClient.GetDatabase(Program.MONGO_DB_DBN);
        var collection = database.GetCollection<BsonDocument>("current_game_data");
        var filter = Builders<BsonDocument>.Filter.Eq("siteid", SiteId);
        filter &= Builders<BsonDocument>.Filter.Eq("gameid", GameId);

        var fieldsBuilder = Builders<BsonDocument>.Projection;
        var fields = fieldsBuilder.Exclude("_id");

        var data = collection.Find(filter).Project<BsonDocument>(fields).FirstOrDefault();
        if (data != null)
        {
          var dataObj = JObject.Parse(data.ToString());
          foreach (var k in dataObj)
          {
            UpdateCurrentGameData(SiteId, GameId, new JProperty(k.Key.ToString(), k.Value), null, false);
          }
        }
      }
    }

    public static async Task GetQuizData(long SiteId, long GameId)
    {
      var dbClient = new MongoClient(Program.MONGO_DB_SVR);
      var database = dbClient.GetDatabase(Program.MONGO_DB_DBN);
      var collection = database.GetCollection<BsonDocument>("quizzes");
      var filter = Builders<BsonDocument>.Filter.Eq("siteid", SiteId);
      filter &= Builders<BsonDocument>.Filter.Eq("quizid", GameId);

      var fieldsBuilder = Builders<BsonDocument>.Projection;
      var fields = fieldsBuilder.Exclude("_id");

      var data = collection.Find(filter).Project<BsonDocument>(fields).FirstOrDefault();

      if (data != null)
      {

        var dataAsObj = JObject.Parse(data.ToString());

        if (dataAsObj.ContainsKey("rounds"))
        {
          var roundcollection = database.GetCollection<BsonDocument>("rounds");
          var roundfilter = Builders<BsonDocument>.Filter.Eq("siteid", (int)SiteId);

          List<int> QuizRounds = new List<int>();
          List<string> QuizRoundsOrder = new List<string>();

          foreach (var k in dataAsObj["rounds"])
          {
            var i = (int)k;
            QuizRounds.Add(i);
            QuizRoundsOrder.Add(k.ToString());
          }

          roundfilter &= Builders<BsonDocument>.Filter.AnyIn("roundid", QuizRounds);

          var roundfieldsBuilder = Builders<BsonDocument>.Projection;
          var roundfields = roundfieldsBuilder.Exclude("_id");
          var rounddata = roundcollection.Find(roundfilter).Project(roundfields).ToList();

          if (rounddata.Count > 0)
          {
            dataAsObj.Remove("rounds");
            var obj = new JArray();

            foreach (var row in rounddata)
            {
              var rowData = JObject.Parse(row.ToString());
              if (rowData.ContainsKey("questions"))
              {
                var questioncollection = database.GetCollection<BsonDocument>("questions");
                var questionfilter = Builders<BsonDocument>.Filter.Eq("siteid", (int)SiteId);

                List<int> RoundQuestions = new List<int>();

                foreach (var k in rowData["questions"])
                {
                  var i = (int)k;
                  RoundQuestions.Add(i);
                }

                questionfilter &= Builders<BsonDocument>.Filter.AnyIn("questionid", RoundQuestions);

                var questionfieldsBuilder = Builders<BsonDocument>.Projection;
                var questionfields = questionfieldsBuilder.Exclude("_id").Exclude("siteid");
                var questiondata = questioncollection.Find(questionfilter).Project(questionfields).ToList();
                var questionObj = new JArray();

                if (questiondata.Count > 0)
                {
                  var questionIndex = 0;
                  rowData.Remove("questions");
                  foreach (var k in questiondata)
                  {
                    questionIndex++;
                    var thisQuestionObj = JObject.Parse(k.ToString());
                    if (thisQuestionObj.ContainsKey("questionIndex"))
                    {
                      thisQuestionObj["questionIndex"].Parent.Remove();
                    }
                    thisQuestionObj.Add(new JProperty("questionIndex", questionIndex));
                    questionObj.Add(thisQuestionObj);
                  }
                  rowData.Add("questions", questionObj);
                }

                rowData.Add("questioncount", rowData.ContainsKey("questions") ? rowData["questions"].Count() : 0);
              }
              obj.Add(rowData);
            }

            var newObj = new JArray();
            var pos = 0;
            for (var i=0; i < QuizRoundsOrder.Count(); i++)
            {
              var roundId = long.Parse(QuizRoundsOrder[i].ToString());
              for (var k = 0; k < obj.Count; k++)
              {
                if(long.Parse(obj[k]["roundid"].ToString()) == roundId) {
                  newObj.Add(obj[k]);
                }
                pos++;
              }
            }
            dataAsObj.TryAdd("rounds", newObj);
          }
        }

        if (!LoadedQuizzes.ContainsKey(SiteId))
        {
          LoadedQuizzes[SiteId] = new ConcurrentDictionary<string, ConcurrentDictionary<long, JObject>>();
          LoadedQuizzes[SiteId].TryAdd("full", new ConcurrentDictionary<long, JObject>());
          LoadedQuizzes[SiteId].TryAdd("min", new ConcurrentDictionary<long, JObject>());
        }
        LoadedQuizzes[SiteId]["full"][GameId] = dataAsObj;

        var minDataAsObj = JObject.Parse(dataAsObj.ToString());
        for (int i = minDataAsObj["rounds"].Count() - 1; i >= 0; i--)
        {
          var questionNode = minDataAsObj["rounds"][i]["questions"];
          var roundMod = JObject.Parse((minDataAsObj["roundmodifiers"].ToString() == "" ? "{}" : minDataAsObj["roundmodifiers"].ToString()));
          var roundId = minDataAsObj["rounds"][i]["roundid"].ToString();
          var thisRoundMod = JObject.Parse(roundMod.ContainsKey(roundId) ? roundMod[roundId].ToString() : "{}");
          var thisRoundAlwaysAvailable = (thisRoundMod.ContainsKey("round-mod-always-available") && thisRoundMod["round-mod-always-available"].ToString().ToLower() == "true");
          if (thisRoundAlwaysAvailable)
          {
            for (int a = 0; a < questionNode.Count(); a++)
            {
              var question = questionNode[a];
              if (question["answers"].Count() <= 1 || question["answers"].Count() == question["correctanswers"].Count()) {
                question["answers"].Parent.Remove();
                question["answerimages"].Parent.Remove();
              }
              if(question["questionimage"].ToString() == "") {
                question["questionimage"].Parent.Remove();
              }
              question["correctanswers"].Parent.Remove();
            }
          }
          else
          {
            questionNode.Parent.Remove();
          }
        }

        LoadedQuizzes[SiteId]["min"][GameId] = minDataAsObj;

        var scoreNode = LoadedQuizzes[SiteId]["full"][GameId].GetValue("roundmodifiers");
        var roundNode = LoadedQuizzes[SiteId]["min"][GameId].GetValue("rounds");

        if(!CurrentGameData.ContainsKey(SiteId)) {
          CurrentGameData.TryAdd(SiteId, new ConcurrentDictionary<long, JObject>());
        }
        if(!CurrentGameData[SiteId].ContainsKey(GameId)) {
          CurrentGameData[SiteId].TryAdd(GameId, new JObject());
        }
        CurrentGameData[SiteId][GameId]["rounds"] = roundNode;
        var scoreNodeJson = WebUtility.HtmlDecode(scoreNode.ToString());
        CurrentGameData[SiteId][GameId]["scoring"] = JObject.Parse(scoreNodeJson != "" ? scoreNodeJson : "{}");

        return;
      }
    }

    public static async Task CorrectAnswerScore(long SiteId, long GameId, long UserId, long RoundId, long QuestionId, JObject roundScoring, ConnectedClient client = null)
    {
      var correctAnswerScoreInt = (long)(roundScoring.ContainsKey("round-mod-score-correct") ? roundScoring["round-mod-score-correct"].Value<long>() : 0);
      var incorrectAnswerScoreInt = (long)(roundScoring.ContainsKey("round-mod-score-incorrect") ? roundScoring["round-mod-score-incorrect"].Value<long>() : 0);

      var oldData = JObject.Parse(CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()][QuestionId.ToString()].ToString());
      var correctAnswers = new JArray();
      if (oldData.ContainsKey("incorrectanswers"))
      {
        try
        {
          correctAnswers = JArray.Parse(oldData["incorrectanswers"].ToString());
        } catch (Exception)
        {
          correctAnswers = new JArray(new JValue(oldData["incorrectanswers"].ToString()));
        }
      }
      var incorrectAnswers = new JArray();

      if (client != null)
      {
        client.LastScoreAwarded = correctAnswerScoreInt;
        client.LastScoresCorrect = correctAnswers;
        client.LastScoresIncorrect = incorrectAnswers;
      }


      if (CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()] == null)
      {
        CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()] = new JObject();
      }

      if(CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()][QuestionId.ToString()] == null) {
        CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()][QuestionId.ToString()] = new JObject();
      }
      var amended = JObject.Parse(CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()][QuestionId.ToString()].ToString());
      var newOutput = new JObject(new JProperty("speedscoreindex", 0), new JProperty("correctanswers", correctAnswers), new JProperty("awardedscore", correctAnswerScoreInt), new JProperty("incorrectanswers", incorrectAnswers), new JProperty("autoscorer", !(!roundScoring.ContainsKey("round-mod-auto-scorer") || roundScoring["round-mod-auto-scorer"].Value<Boolean>() == false)), new JProperty("amended", true), new JProperty("userid", UserId), new JProperty("score", correctAnswerScoreInt), new JProperty("amendedscore", correctAnswerScoreInt), new JProperty("questionid", QuestionId), new JProperty("roundid", RoundId));
      if (!amended.ContainsKey("amended"))
      {
        CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()] = (CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()].Value<long>() - incorrectAnswerScoreInt) + correctAnswerScoreInt;
        CurrentGameData[SiteId][GameId]["allusers"][UserId.ToString()]["score"] = CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()];
        CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()][QuestionId.ToString()] = newOutput;
      }
      else
      {
        newOutput = JObject.Parse(CurrentGameData[SiteId][GameId]["teamsanswered"][UserId.ToString()][RoundId.ToString()][QuestionId.ToString()].ToString());
      }


      var outputObject = new JObject();
      outputObject.Add(new JProperty(UserId.ToString(), newOutput));
      outputObject.Add(new JProperty("all", new JObject(new JProperty("gameupdate", new JObject(new JProperty("userscore", new JObject(new JProperty("userid", UserId), new JProperty("score", CurrentGameData[SiteId][GameId]["leaderboard"][UserId.ToString()]))))))));
      outputObject.Add(new JProperty("quizmaster", new JObject(new JProperty("request", new JObject(new JProperty("callback", "mc_teamanswered_cb"))), new JProperty("gameupdate", new JObject(new JProperty("teamsanswered", new JObject(new JProperty(UserId.ToString(), new JObject(new JProperty(RoundId.ToString(), new JObject(new JProperty(QuestionId.ToString(), newOutput))))))))))));

      outputObject[UserId.ToString()]["type"] = "answer-submit";

      await BroadcastToMultiple(GameClients[SiteId][GameId], outputObject);

      return;
    }

    public static void UpdateCurrentGameData(long SiteId, long GameId, JProperty data, JObject request = null, bool sendUpdate = true)
    {
      if (!CurrentGameData.ContainsKey(SiteId))
      {
        CurrentGameData.TryAdd(SiteId, new ConcurrentDictionary<long, JObject>());
      }
      if (!CurrentGameData[SiteId].ContainsKey(GameId))
      {
        CurrentGameData[SiteId].TryAdd(GameId, new JObject());
      }
      
      if(data.Name.ToString() != "")
      {
        if(CurrentGameData[SiteId][GameId].ContainsKey(data.Name.ToString()))
        {
          CurrentGameData[SiteId][GameId][data.Name].Parent.Remove();
        }

        CurrentGameData[SiteId][GameId].TryAdd(data.Name.ToString(), data.Value);
      }

      if (sendUpdate)
      {
        BroadcastToAll(GameClients[SiteId][GameId], new JObject(new JProperty("gameupdate", new JObject(data)), new JProperty("request", request)).ToString());
      }
    }

    public static async Task InitialiseTimer(long SiteId, long GameId, JObject message, int seconds, int afterAction)
    {
      await Task.Run(async () => //Task.Run automatically unwraps nested Task types!
      {
        await Task.Delay(seconds * 1000);
        UpdateCurrentGameData(SiteId, GameId, new JProperty("timerrunning", null), message);

        if (afterAction == 1 || afterAction > 3)
        {
          UpdateCurrentGameData(SiteId, GameId, new JProperty("question", null), message);
        }
        else
        {
          var GameData = LoadedQuizzes[SiteId]["full"][GameId];
          var LocalCurrentGameData = CurrentGameData[SiteId][GameId];
          if (afterAction == 2)
          {
            var currentRoundObj = JObject.Parse(LocalCurrentGameData["round"].ToString());
            var currentRoundId = currentRoundObj.ContainsKey("roundid") ? currentRoundObj.Value<int>("roundid") : 0;
            var currentQuestionObj = JObject.Parse(LocalCurrentGameData["question"].ToString());
            var currentQuestionId = currentQuestionObj.ContainsKey("questionid") ? currentQuestionObj.Value<int>("questionid") : 0;

            var questionObj = new JObject();
            var continueLoop = true;
            var foundQuestion = false;
            UpdateCurrentGameData(SiteId, GameId, new JProperty("question", null), message);

            for (var i = 0; i < GameData["rounds"].Count(); i++)
            {
              if (GameData["rounds"][i].Value<int>("roundid") == currentRoundId)
              {
                var foundRound = JObject.Parse(GameData["rounds"][i].ToString());
                if (foundRound.ContainsKey("questions"))
                {
                  for (var io = 0; io < foundRound["questions"].Count(); io++)
                  {
                    if (foundRound["questions"][io] != null)
                    {
                      var question = JObject.Parse(foundRound["questions"][io].ToString());
                      if (foundQuestion)
                      {
                        questionObj = question;
                        continueLoop = false;
                        break;
                      }
                      if (question.ContainsKey("questionid") && question.Value<int>("questionid") == currentQuestionId)
                      {
                        foundQuestion = true;
                      }
                    }
                  }
                }
                if (!continueLoop)
                {
                  break;
                }
              }
            }
            if (foundQuestion && questionObj.GetValue("questionid") != null)
            {
              if (questionObj.ContainsKey("answers"))
              {
                var answerNode = (JArray)questionObj["answers"];
                if (answerNode.Count < 2)
                {
                  questionObj["answers"].Parent.Remove();
                }
              }

              if (questionObj.ContainsKey("answerimages"))
              {
                var answerImagesNode = (JArray)questionObj["answerimages"];
                if (answerImagesNode.Count < 1)
                {
                  questionObj["answerimages"].Parent.Remove();
                }
              }

              if (questionObj.ContainsKey("correctanswers"))
              {
                questionObj["correctanswers"].Parent.Remove();
              }
              if (questionObj.ContainsKey("questionimage") && questionObj["questionimage"].Value<string>() == "")
              {
                questionObj["questionimage"].Parent.Remove();
              }

              if (questionObj.ContainsKey("category"))
              {
                questionObj["category"].Parent.Remove();
              }

              if (!CurrentGameData[SiteId][GameId].ContainsKey("seenquestions"))
              {
                CurrentGameData[SiteId][GameId]["seenquestions"] = new JObject();
              }

              var roundid = currentRoundId;
              var questionid = questionObj.GetValue("questionid");
              var newKey = roundid + "-" + questionid;

              CurrentGameData[SiteId][GameId]["seenquestions"][newKey] = true;

              UpdateCurrentGameData(SiteId, GameId, new JProperty("seenquestions", CurrentGameData[SiteId][GameId]["seenquestions"]));
              UpdateCurrentGameData(SiteId, GameId, new JProperty("question", questionObj), message);
            }
            else
            {
              afterAction = 3;
            }
          }

          if (afterAction == 3)
          {
            UpdateCurrentGameData(SiteId, GameId, new JProperty("question", null), message);
            UpdateCurrentGameData(SiteId, GameId, new JProperty("round", null), message);
          }
        }
      });
    }

    public static async Task<bool> RemoveGameData(long SiteId, long GameId, bool SkipDatabaseUpdate=false)
    {
      Console.WriteLine($"Site {SiteId} / Game {GameId}: removing game data.");
      if (GameClients.ContainsKey(SiteId))
      {
        if(GameClients[SiteId].ContainsKey(GameId))
        {
          foreach(var kvp in GameClients[SiteId][GameId])
          {
            var outputMessage = kvp.Value.WriteClientError("This game has now expired, press the button below to return to the home screen.", true);
            var msgbuf = new ArraySegment<byte>(Encoding.UTF8.GetBytes(outputMessage["sendToSelf"].ToString()));
            await kvp.Value.Socket.SendAsync(msgbuf, WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);
            kvp.Value.Socket.Dispose();
            RemoveClientFromGame(SiteId, GameId, kvp.Value);
          }
        }
      }
      if (LoadedQuizzes.ContainsKey(SiteId))
      {
        if (LoadedQuizzes[SiteId].ContainsKey("full"))
        {
          if (LoadedQuizzes[SiteId]["full"].ContainsKey(GameId))
          {
            var loadedFullNode = LoadedQuizzes[SiteId]["full"];
            loadedFullNode.TryRemove(GameId, out _);
          }
          if (LoadedQuizzes[SiteId]["full"].Count == 0)
          {
            LoadedQuizzes[SiteId].TryRemove("full", out _);
          }
        }
        if (LoadedQuizzes[SiteId].ContainsKey("min"))
        {
          if (LoadedQuizzes[SiteId]["min"].ContainsKey(GameId))
          {
            var loadedMinNode = LoadedQuizzes[SiteId]["min"];
            loadedMinNode.TryRemove(GameId, out _);
          }
          if(LoadedQuizzes[SiteId]["min"].Count == 0)
          {
            LoadedQuizzes[SiteId].TryRemove("min", out _);
          }
        }
        if(LoadedQuizzes[SiteId].Count == 0)
        {
          LoadedQuizzes.TryRemove(SiteId, out _);
        }
      }
      if(CurrentGameData.ContainsKey(SiteId))
      {
        if(CurrentGameData[SiteId].ContainsKey(GameId))
        {
          CurrentGameData[SiteId].TryRemove(GameId, out _);
        }
        if(CurrentGameData[SiteId].Count == 0)
        {
          CurrentGameData.TryRemove(SiteId, out _);
        }
      }
      if (!SkipDatabaseUpdate)
      {
        JObject packet = new JObject(
                             new JProperty("collectionname", "quizzes"),
                             new JProperty("action", "update"),
                             new JProperty("data", new JObject(new JProperty("field", "ended"), new JProperty("value", true))),
                             new JProperty("packetfilter", new JObject(
                                 new JProperty("siteid", SiteId),
                                 new JProperty("quizid", GameId)))
                          );
        dbActions.Insert(0, packet);
      }
      return true;
    }

    private static async Task CloseAllSocketsAsync()
    {
      // We can't dispose the sockets until the processing loops are terminated,
      // but terminating the loops will abort the sockets, preventing graceful closing.
      var disposeQueue = new List<WebSocket>(AllClients.Count);

      while (AllClients.Count > 0)
      {
        var client = AllClients.ElementAt(0).Value;
        Console.WriteLine($"Closing Socket {client.SocketId}");

        Console.WriteLine("... ending broadcast loop");
        client.BroadcastLoopTokenSource.Cancel();

        if (client.Socket.State != WebSocketState.Open)
        {
          Console.WriteLine($"... socket not open, state = {client.Socket.State}");
        }
        else
        {
          var timeout = new CancellationTokenSource(Program.CLOSE_SOCKET_TIMEOUT_MS);
          try
          {
            Console.WriteLine("... starting close handshake");
            await client.Socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", timeout.Token);
          }
          catch (OperationCanceledException ex)
          {
            Program.ReportException(ex);
            // normal upon task/token cancellation, disregard
          }
          catch (WebSocketException ex)
          {
            Program.ReportException(ex);
          }
        }

        RemoveClientFromGame(client.SiteId, client.GameId, client);
        UpdateCurrentGameData(client.SiteId, client.GameId, new JProperty("", null), new JObject());
        disposeQueue.Add(client.Socket);

        Console.WriteLine("... done");
      }

      // now that they're all closed, terminate the blocking ReceiveAsync calls in the SocketProcessingLoop threads
      SocketLoopTokenSource.Cancel();

      // dispose all resources
      foreach (var socket in disposeQueue)
        socket.Dispose();
    }

    private static async Task SocketProcessingLoopAsync(ConnectedClient client)
    {
      _ = Task.Run(() => client.BroadcastLoopAsync().ConfigureAwait(false));

      var socket = client.Socket;
      var loopToken = SocketLoopTokenSource.Token;
      var broadcastTokenSource = client.BroadcastLoopTokenSource; // store a copy for use in finally block
      try
      {
        var buffer = WebSocket.CreateServerBuffer(4096);
        while (socket.State != WebSocketState.Closed && socket.State != WebSocketState.Aborted && !loopToken.IsCancellationRequested)
        {
          var receiveResult = await client.Socket.ReceiveAsync(buffer, loopToken);
          var canContinue = GarbageCollection();

          if (canContinue)
          {
            // if the token is cancelled while ReceiveAsync is blocking, the socket state changes to aborted and it can't be used
            if (!loopToken.IsCancellationRequested)
            {
              // the client is notifying us that the connection will close; send acknowledgement
              if (client.Socket.State == WebSocketState.CloseReceived && receiveResult.MessageType == WebSocketMessageType.Close)
              {
                Console.WriteLine($"Socket {client.SocketId}: Acknowledging Close frame received from client");
                RemoveClientFromGame(client.SiteId, client.GameId, client);
                broadcastTokenSource.Cancel();
                await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Acknowledge Close frame", CancellationToken.None);
                // the socket state changes to closed at this point
              }

              // echo text or binary data to the broadcast queue
              if (client.Socket.State == WebSocketState.Open)
              {
                Console.WriteLine($"Socket {client.SocketId}: Received {receiveResult.MessageType} frame ({receiveResult.Count} bytes).");
                var authObj = new ConcurrentDictionary<string, JObject>();
                var localMsg = Encoding.UTF8.GetString(buffer.Array, 0, receiveResult.Count);
                if (localMsg != "[object Object]")
                {
                  JObject message = JObject.Parse(localMsg);
                  if (client.IsAuthed)
                  {
                    Console.WriteLine($"Socket {client.SocketId}: Message received.");
                    var messageType = message.ContainsKey("type") ? message["type"].ToString() : null;
                    var messageCmd = message.ContainsKey("command") ? message["command"].ToString() : null;
                    var messageData = message.ContainsKey("data") ? message["data"] : null;
                    var messageUser = message.ContainsKey("user") ? message["user"] : null;
                    var messageSiteId = message.ContainsKey("siteid") ? message["siteid"].ToString() : null;
                    long localSiteId = client.SiteId;
                    long localGameId = client.GameId;

                    var output = await client.HandleMessageAsync(message, messageType, messageCmd, messageData, messageUser, messageSiteId);
                    var messageOutput = "";

                    if(CurrentGameData.ContainsKey(localSiteId) && CurrentGameData[localSiteId].ContainsKey(localGameId) && !CurrentGameData[localSiteId][localGameId].ContainsKey("ended"))
                    {
                      var expiryTime = DateTime.Now.AddHours(01);
                      Console.WriteLine("Site " + localSiteId + " / Game " + localGameId + ": Expiry Time set - " + expiryTime.ToString());
                      CurrentGameData[localSiteId][localGameId]["expirytime"] = expiryTime.ToString();
                    }

                    if (output.ContainsKey("sendToAll"))
                    {
                      if (!output["sendToAll"].ContainsKey("request"))
                      {
                        output["sendToAll"].Add(new JProperty("request", message));
                      }
                      messageOutput = output["sendToAll"].ToString();
                      StoreQuizPacket(client, "All", message, output["sendToAll"]);
                      BroadcastToAll(GameClients[localSiteId][localGameId], messageOutput);
                    }

                    if (output.ContainsKey("sendToPlayers"))
                    {
                      if (!output["sendToPlayers"].ContainsKey("request"))
                      {
                        output["sendToPlayers"].Add(new JProperty("request", message));
                      }
                      messageOutput = output["sendToPlayers"].ToString();
                      StoreQuizPacket(client, "Players", message, output["sendToPlayers"]);
                      BroadcastToAll(GameClients[localSiteId][localGameId], messageOutput, "players");
                    }

                    if (output.ContainsKey("sendToQuizMaster"))
                    {
                      if (!output["sendToQuizMaster"].ContainsKey("request"))
                      {
                        output["sendToQuizMaster"].Add(new JProperty("request", message));
                      }
                      messageOutput = output["sendToQuizMaster"].ToString();
                      StoreQuizPacket(client, "QuizMaster", message, output["sendToQuizMaster"]);
                      BroadcastToAll(GameClients[localSiteId][localGameId], messageOutput, "quizmaster");
                    }

                    if (output.ContainsKey("sendToSelf"))
                    {
                      if (!output["sendToSelf"].ContainsKey("request"))
                      {
                        output["sendToSelf"].Add(new JProperty("request", message));
                      }
                      messageOutput = output["sendToSelf"].ToString();
                      client.BroadcastQueue.Writer.TryWrite(messageOutput);
                    }
                  }
                  else
                  {
                    Console.WriteLine($"Socket {client.SocketId}: Attempting to auth.");
                    authObj = client.AuthClient(message);
                    var shouldProceed = true;
                    if (!authObj.ContainsKey("sendToSelf"))
                    {
                      var loadQuizData = true;

                      if (LoadedQuizzes.ContainsKey(client.SiteId))
                      {
                        if (LoadedQuizzes[client.SiteId].ContainsKey("full"))
                        {
                          if (LoadedQuizzes[client.SiteId]["full"].ContainsKey(client.GameId))
                          {
                            loadQuizData = false;
                          }
                        }
                      }

                      if (loadQuizData)
                      {
                        await LoadCurrentGameData(client.SiteId, client.GameId);
                        await GetQuizData(client.SiteId, client.GameId);
                      }
                      if (CurrentGameData.ContainsKey(client.SiteId) && CurrentGameData[client.SiteId].ContainsKey(client.GameId) && CurrentGameData[client.SiteId][client.GameId].ContainsKey("expirytime"))
                      {
                        var now = DateTime.Now.ToLocalTime();
                        var expiryTime = DateTime.Parse(CurrentGameData[client.SiteId][client.GameId]["expirytime"].Value<string>());
                        if (expiryTime < now)
                        {
                          ConcurrentDictionary<string, JObject> localError = client.WriteClientError("This game has now expired, press the button below to return to the home screen.", true);
                          localError["sendToSelf"].Add(new JProperty("request", message));

                          var msgbuf = new ArraySegment<byte>(Encoding.UTF8.GetBytes(localError["sendToSelf"].ToString()));
                          await client.Socket.SendAsync(msgbuf, WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);
                          client.Socket.Dispose();
                          shouldProceed = false;
                          Console.WriteLine($"Socket {client.SocketId}: Game expired.");
                          client.IsAuthed = true;

                          RemoveGameData(client.SiteId, client.GameId, true);
                        }
                      }
                      if (shouldProceed)
                      {
                        if (!GameClients.ContainsKey(client.SiteId))
                        {
                          var siteid = client.SiteId;
                          GameClients.TryAdd(client.SiteId, new ConcurrentDictionary<long, ConcurrentDictionary<long, ConnectedClient>>());
                        }

                        if (client.GameId > 0)
                        {
                          if (!GameClients[client.SiteId].ContainsKey(client.GameId))
                          {
                            GameClients[client.SiteId][client.GameId] = new ConcurrentDictionary<long, ConnectedClient>();
                          }

                          GameClients[client.SiteId][client.GameId][client.UserId] = client;

                          var playerCount = GameClients[client.SiteId][client.GameId].Count;
                          Console.WriteLine($"Socket {client.SocketId}: not already connected.");
                          client.IsAuthed = true;

                          if (CurrentGameData.ContainsKey(client.SiteId) && CurrentGameData[client.SiteId].ContainsKey(client.GameId))
                          {

                            if (CurrentGameData[client.SiteId][client.GameId].ContainsKey("teamsanswered") && CurrentGameData[client.SiteId][client.GameId]["teamsanswered"][client.UserId.ToString()] != null)
                            {
                              client.BroadcastQueue.Writer.TryWrite(new JObject(new JProperty("answeredquestions", CurrentGameData[client.SiteId][client.GameId]["teamsanswered"][client.UserId.ToString()])).ToString());
                            }
                            else
                            {
                              if (!CurrentGameData[client.SiteId][client.GameId].ContainsKey("teamsanswered"))
                              {
                                CurrentGameData[client.SiteId][client.GameId]["teamsanswered"] = new JObject();
                              }
                            }

                            if (LoadedQuizzes.ContainsKey(client.SiteId))
                            {
                              if (client.IsQuizMaster && LoadedQuizzes[client.SiteId].ContainsKey("full") && LoadedQuizzes[client.SiteId]["full"].ContainsKey(client.GameId))
                              {
                                client.BroadcastQueue.Writer.TryWrite(new JObject(new JProperty("gametimeline", LoadedQuizzes[client.SiteId]["full"][client.GameId])).ToString());
                              }
                              else if (LoadedQuizzes[client.SiteId].ContainsKey("min") && LoadedQuizzes[client.SiteId]["min"].ContainsKey(client.GameId) && CurrentGameData[client.SiteId][client.GameId].ContainsKey("started") && CurrentGameData[client.SiteId][client.GameId].Value<bool>("started"))
                              {
                                client.BroadcastQueue.Writer.TryWrite(new JObject(new JProperty("gametimeline", LoadedQuizzes[client.SiteId]["min"][client.GameId])).ToString());
                              }
                            }
                            var currentData = JObject.Parse(CurrentGameData[client.SiteId][client.GameId].ToString());
                            if (currentData.ContainsKey("teamsanswered"))
                            {
                              currentData.Remove("teamsanswered");
                            }
                            client.BroadcastQueue.Writer.TryWrite(new JObject(new JProperty("currentdata", currentData)).ToString());
                            var teamScore = 0;
                            var leaderboard = (CurrentGameData[client.SiteId][client.GameId].ContainsKey("leaderboard") ? CurrentGameData[client.SiteId][client.GameId]["leaderboard"].ToString() : "{}");
                            var leaderboardObj = JObject.Parse(leaderboard.ToString());
                            if (leaderboardObj.ContainsKey(client.UserId.ToString()))
                            {
                              teamScore = int.Parse(leaderboardObj[client.UserId.ToString()].ToString());
                            }
                            var userJoinData = new JObject(new JProperty("userid", client.UserId), new JProperty("online", true), new JProperty("username", client.TeamName), new JProperty("avatarurl", client.AvatarURL), new JProperty("muted", client.IsMuted), new JProperty("mutereason", client.MuteReason), new JProperty("score", teamScore), new JProperty("player", (!client.IsAdmin && !client.IsQuizMaster)));
                            var outputClients = new JObject();
                            if (!CurrentGameData[client.SiteId][client.GameId].ContainsKey("allusers"))
                            {
                              CurrentGameData[client.SiteId][client.GameId]["allusers"] = new JObject();
                            }

                            if (!LoadedQuizzes[client.SiteId]["full"][client.GameId].ContainsKey("teamsanswered"))
                            {
                              LoadedQuizzes[client.SiteId]["full"][client.GameId].Add("teamsanswered", new JObject());
                            }

                            if (!client.IsAdmin && !client.IsQuizMaster)
                            {
                              if (CurrentGameData[client.SiteId][client.GameId]["teamsanswered"][client.UserId.ToString()] == null)
                              {
                                CurrentGameData[client.SiteId][client.GameId]["teamsanswered"][client.UserId.ToString()] = new JObject();
                              }
                              var allJoinedUsers = new JObject();
                              if (!CurrentGameData[client.SiteId][client.GameId].ContainsKey("connectedusers"))
                              {
                                CurrentGameData[client.SiteId][client.GameId]["connectedusers"] = new JObject();
                              }
                              CurrentGameData[client.SiteId][client.GameId]["connectedusers"][client.UserId.ToString()] = userJoinData;
                              UpdateCurrentGameData(client.SiteId, client.GameId, new JProperty("connectedusers", CurrentGameData[client.SiteId][client.GameId]["connectedusers"]), message);

                              if (LoadedQuizzes[client.SiteId]["full"][client.GameId]["teamsanswered"][client.UserId.ToString()] == null)
                              {
                                LoadedQuizzes[client.SiteId]["full"][client.GameId]["teamsanswered"][client.UserId.ToString()] = new JObject();
                              }

                              if (!CurrentGameData[client.SiteId][client.GameId].ContainsKey("leaderboard"))
                              {
                                CurrentGameData[client.SiteId][client.GameId]["leaderboard"] = new JObject();
                              }

                              if (CurrentGameData[client.SiteId][client.GameId]["leaderboard"][client.UserId.ToString()] == null)
                              {
                                CurrentGameData[client.SiteId][client.GameId]["leaderboard"][client.UserId.ToString()] = (long)0;
                              }
                            }

                            CurrentGameData[client.SiteId][client.GameId]["allusers"][client.UserId.ToString()] = userJoinData;
                            UpdateCurrentGameData(client.SiteId, client.GameId, new JProperty("allusers", CurrentGameData[client.SiteId][client.GameId]["allusers"]), message);
                          }
                        }
                      }
                    }
                  }

                  if (!client.IsAuthed)
                  {
                    Console.WriteLine($"Socket {client.SocketId}: auth failed.");
                    if (authObj.ContainsKey("sendToSelf"))
                    {
                      client.BroadcastQueue.Writer.TryWrite(authObj["sendToSelf"].ToString());
                      client.Socket.Dispose();
                    }
                    else
                    {
                      AllClients.TryRemove(client.SocketId.ToString(), out _);
                      client.Socket.Dispose();
                    }
                  }
                }
              }
            }
          }
        }
      }
      catch (OperationCanceledException)
      {
        // normal upon task/token cancellation, disregard
      }
      catch (WebSocketException ex)
      {
        Console.WriteLine($"Socket {client.SocketId}: WebSocket exception - " + ex.Message);
      }
      catch (Exception ex)
      {
        Console.WriteLine($"Socket {client.SocketId}:");
        Console.WriteLine(ex.StackTrace.ToString());
        Program.ReportException(ex);

        // don't leave the socket in any potentially connected state
        if (client.Socket.State != WebSocketState.Closed)
          client.Socket.Abort();

        // by this point the socket is closed or aborted, the ConnectedClient object is useless
        RemoveClientFromGame(client.SiteId, client.GameId, client);

        socket.Dispose();
      }
      finally
      {
        broadcastTokenSource.Cancel();

        Console.WriteLine($"Socket {client.SocketId}: Ended processing loop in state {socket.State}");

        // don't leave the socket in any potentially connected state
        if (client.Socket.State != WebSocketState.Closed)
          client.Socket.Abort();

        // by this point the socket is closed or aborted, the ConnectedClient object is useless
        RemoveClientFromGame(client.SiteId, client.GameId, client);

        socket.Dispose();

        // signal to the middleware pipeline that this task has completed
        client.TaskCompletion.SetResult(true);
      }
    }
    public static bool GarbageCollection()
    {
      var now = DateTime.Now;
      if (now > GCLastTime)
      {
        Console.WriteLine("MemCheck: Cleaning up memory.");
        GC.KeepAlive(LoadedQuizzes);
        GC.KeepAlive(CurrentGameData);
        GC.KeepAlive(AllClients);
        GC.KeepAlive(GameClients);
        GC.KeepAlive(QuestionAnswerCount);
        GC.KeepAlive(AnsweredQuestions);
        GC.KeepAlive(ChatData);
        GC.KeepAlive(dbActions);
        GC.KeepAlive(SocketLoopTokenSource);
        GC.KeepAlive(ServerIsRunning);
        GC.KeepAlive(GCLastTime);
        GC.KeepAlive(AppShutdownHandler);
        GC.Collect();
        GCLastTime = DateTime.Now.AddSeconds(30);
      } 
      else
      {
        Console.WriteLine("MemCheck: Memory OK.");
      }
      return true;
    }
  }
}

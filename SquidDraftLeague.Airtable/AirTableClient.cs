using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using AirtableApiClient;
using Newtonsoft.Json.Linq;
using NLog;
using SquidDraftLeague.Draft;
using SquidDraftLeague.Draft.Map;
using SquidDraftLeague.Settings;

namespace SquidDraftLeague.AirTable
{
    public static class AirTableClient
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public static async Task<bool> CheckHasPlayedSet(SdlPlayer player)
        {
            string offset = null;
            string errorMessage = null;
            List<AirtableRecord> records = new List<AirtableRecord>();

            using (AirtableBase airtableBase = new AirtableBase(Globals.BotSettings.AppKey, Globals.BotSettings.BaseId))
            {
                do
                {
                    Logger.Info($"Retrieving data with offset {offset}.");

                    Task<AirtableListRecordsResponse> task = airtableBase.ListRecords(
                        "Draft Log",
                        offset,
                        null,
                        null,
                        null,
                        null
                    );

                    AirtableListRecordsResponse response = await task;

                    if (response.Success)
                    {
                        Logger.Info($"Success! Continuing with offset \"{response.Offset}\"");
                        records.AddRange(response.Records.ToList());
                        offset = response.Offset;
                    }
                    else if (response.AirtableApiError != null)
                    {
                        errorMessage = response.AirtableApiError.ErrorMessage;
                        break;
                    }
                    else
                    {
                        errorMessage = "Unknown error";
                        break;
                    }
                } while (offset != null);
            }

            if (!string.IsNullOrEmpty(errorMessage))
            {
                SdlAirTableException airTableException = new SdlAirTableException(
                    errorMessage, SdlAirTableException.AirtableErrorType.CommunicationError);
                Logger.Error(airTableException);
                throw airTableException;
            }

            return records.Any(x =>
                ((JArray) x.Fields["Alpha Players"]).Any(y => y.Value<string>() == player.AirtableId) ||
                ((JArray) x.Fields["Bravo Players"]).Any(y => y.Value<string>() == player.AirtableId));
        }

        public static async Task SetRoleAsync(SdlPlayer player, string role)
        {
            using (AirtableBase airtableBase = new AirtableBase(Globals.BotSettings.AppKey, Globals.BotSettings.BaseId))
            {
                Fields fields = new Fields();
                fields.AddField("Role", role);

                AirtableCreateUpdateReplaceRecordResponse response =
                    await airtableBase.UpdateRecord("Draft Standings", fields, player.AirtableId, true);

                if (!response.Success)
                {
                    Logger.Error(response.AirtableApiError.ErrorMessage);
                }
            }
        }

        public static async Task SetFriendCodeAsync(SdlPlayer player, string code)
        {
            using (AirtableBase airtableBase = new AirtableBase(Globals.BotSettings.AppKey, Globals.BotSettings.BaseId))
            {
                Fields fields = new Fields();
                fields.AddField("Friend Code", code);

                AirtableCreateUpdateReplaceRecordResponse response =
                    await airtableBase.UpdateRecord("Draft Standings", fields, player.AirtableId, true);

                if (!response.Success)
                {
                    Logger.Error(response.AirtableApiError.ErrorMessage);
                }
            }
        }

        public static async Task<(int Placement, string Ordinal)> GetPlayerStandings(SdlPlayer player)
        {
            AirtableRecord[] allPlayerRecords = await GetAllPlayerRecords();
            List<double> orderedPlayers = allPlayerRecords
                .Select(e => Convert.ToDouble(e.Fields["Power"].ToString()))
                .OrderByDescending(e => e)
                .ToList();

            int placement = -1;

            for (int i = 0; i < orderedPlayers.Count; i++)
            {
                if (Math.Abs(orderedPlayers[i] - player.PowerLevel) >= 0.1)
                {
                    continue;
                }

                placement = i + 1;
            }

            return (placement, GetOrdinal(placement));
        }

        private static string GetOrdinal(int num)
        {
            if (num <= 0) return num.ToString();

            switch (num % 100)
            {
                case 11:
                case 12:
                case 13:
                    return "th";
            }

            switch (num % 10)
            {
                case 1:
                    return "st";
                case 2:
                    return "nd";
                case 3:
                    return "rd";
                default:
                    return "th";
            }

        }

        public static async Task RegisterPlayer(ulong discordId, double startingPowerLevel, string nickname)
        {
            using (AirtableBase airtableBase = new AirtableBase(Globals.BotSettings.AppKey, Globals.BotSettings.BaseId))
            {
                Fields fields = new Fields();
                fields.AddField("Name", nickname);
                fields.AddField("DiscordID", discordId.ToString());
                fields.AddField("Starting Power", startingPowerLevel);

                if ((await GetAllPlayerRecords()).All(e =>
                    e.Fields["DiscordID"].ToString() != discordId.ToString(CultureInfo.InvariantCulture)))
                {
                    AirtableCreateUpdateReplaceRecordResponse response =
                        await airtableBase.CreateRecord("Draft Standings", fields, true);

                    if (!response.Success)
                    {
                        Console.WriteLine(response.AirtableApiError.ErrorMessage);
                    }
                }
            }
        }

        public static async Task ReportScores(Set set, double gain, double loss)
        {
            using (AirtableBase airtableBase = new AirtableBase(Globals.BotSettings.AppKey, Globals.BotSettings.BaseId))
            {
                Fields fields = new Fields();

                string[] alphaPlayers = set.AlphaTeam.Players.Select(e => e.AirtableId).ToArray();
                string[] bravoPlayers = set.BravoTeam.Players.Select(e => e.AirtableId).ToArray();

                fields.AddField("Date", DateTime.Now);
                fields.AddField("Alpha Players", alphaPlayers);
                fields.AddField("Bravo Players", bravoPlayers);
                fields.AddField("Alpha Score", set.AlphaTeam.Score);
                fields.AddField("Bravo Score", set.BravoTeam.Score);
                fields.AddField("Gain", gain);
                fields.AddField("Loss", loss);
                fields.AddField("A SZ",
                    set.AlphaTeam.OrderedMatchResults
                        .Select((e, index) => new { Score = e, set.Stages[index].Mode })
                        .Where(e => e.Score == 1 && e.Mode == GameMode.SplatZones)
                        .Aggregate(0, (e, f) => e + f.Score));
                fields.AddField("B SZ",
                    set.BravoTeam.OrderedMatchResults
                        .Select((e, index) => new { Score = e, set.Stages[index].Mode })
                        .Where(e => e.Score == 1 && e.Mode == GameMode.SplatZones)
                        .Aggregate(0, (e, f) => e + f.Score));
                fields.AddField("A TC",
                    set.AlphaTeam.OrderedMatchResults
                        .Select((e, index) => new { Score = e, set.Stages[index].Mode })
                        .Where(e => e.Score == 1 && e.Mode == GameMode.TowerControl)
                        .Aggregate(0, (e, f) => e + f.Score));
                fields.AddField("B TC",
                    set.BravoTeam.OrderedMatchResults
                        .Select((e, index) => new { Score = e, set.Stages[index].Mode })
                        .Where(e => e.Score == 1 && e.Mode == GameMode.TowerControl)
                        .Aggregate(0, (e, f) => e + f.Score));
                fields.AddField("A RM",
                    set.AlphaTeam.OrderedMatchResults
                        .Select((e, index) => new { Score = e, set.Stages[index].Mode })
                        .Where(e => e.Score == 1 && e.Mode == GameMode.Rainmaker)
                        .Aggregate(0, (e, f) => e + f.Score));
                fields.AddField("B RM",
                    set.BravoTeam.OrderedMatchResults
                        .Select((e, index) => new { Score = e, set.Stages[index].Mode })
                        .Where(e => e.Score == 1 && e.Mode == GameMode.Rainmaker)
                        .Aggregate(0, (e, f) => e + f.Score));
                fields.AddField("A CB",
                    set.AlphaTeam.OrderedMatchResults
                        .Select((e, index) => new { Score = e, set.Stages[index].Mode })
                        .Where(e => e.Score == 1 && e.Mode == GameMode.ClamBlitz)
                        .Aggregate(0, (e, f) => e + f.Score));
                fields.AddField("B CB",
                    set.BravoTeam.OrderedMatchResults
                        .Select((e, index) => new { Score = e, set.Stages[index].Mode })
                        .Where(e => e.Score == 1 && e.Mode == GameMode.ClamBlitz)
                        .Aggregate(0, (e, f) => e + f.Score));

                await airtableBase.CreateRecord("Draft Log", fields, true);
            }
        }

        public static async Task<Stage[]> GetMapList()
        {
            string offset = null;
            string errorMessage = null;
            List<AirtableRecord> records = new List<AirtableRecord>();

            using (AirtableBase airtableBase = new AirtableBase(Globals.BotSettings.AppKey, Globals.BotSettings.BaseId))
            {
                do
                {
                    Logger.Info($"Retrieving data with offset {offset}.");

                    Task<AirtableListRecordsResponse> task = airtableBase.ListRecords(
                        "Map List",
                        offset,
                        null,
                        null,
                        null,
                        null
                    );

                    AirtableListRecordsResponse response = await task;

                    if (response.Success)
                    {
                        Logger.Info($"Success! Continuing with offset \"{response.Offset}\"");
                        records.AddRange(response.Records.ToList());
                        offset = response.Offset;
                    }
                    else if (response.AirtableApiError != null)
                    {
                        errorMessage = response.AirtableApiError.ErrorMessage;
                        break;
                    }
                    else
                    {
                        errorMessage = "Unknown error";
                        break;
                    }
                } while (offset != null);
            }

            if (!string.IsNullOrEmpty(errorMessage))
            {
                SdlAirTableException airTableException = new SdlAirTableException(
                    errorMessage, SdlAirTableException.AirtableErrorType.CommunicationError);
                Logger.Error(airTableException);
                throw airTableException;
            }

            List<Stage> resultStages = new List<Stage>();

            foreach (AirtableRecord airtableRecord in records)
            {
                string mapInfo = airtableRecord.Fields["Name"].ToString();
                string mapName = mapInfo.Substring(0, mapInfo.Length - 3);
                string modeInfo = mapInfo.Substring(mapInfo.Length - 2);

                Stage currentStage = new Stage
                {
                    MapName = mapName,
                    Mode = Stage.GetModeFromAcronym(modeInfo)
                };

                resultStages.Add(currentStage);
            }

            return resultStages.ToArray();
        }

        public static async Task PenalizePlayer(ulong discordId, int points, string notes)
        {
            AirtableRecord playerRecord = await GetPlayerRecord(discordId);

            using (AirtableBase airtableBase = new AirtableBase(Globals.BotSettings.AppKey, Globals.BotSettings.BaseId))
            {
                Fields adjustmentsFields = new Fields();
                adjustmentsFields.AddField("Player", playerRecord.Id);
                adjustmentsFields.AddField("Points", -points);
                adjustmentsFields.AddField("Notes", notes);

                Task<AirtableCreateUpdateReplaceRecordResponse> createRecordTask =
                    airtableBase.CreateRecord("Adjustments", adjustmentsFields, true);

                AirtableCreateUpdateReplaceRecordResponse createRecordResponse = await createRecordTask;

                if (!createRecordResponse.Success)
                {
                    string errorMessage = createRecordResponse.AirtableApiError != null
                        ? createRecordResponse.AirtableApiError.ErrorMessage
                        : "Unknown error";

                    SdlAirTableException exception = new SdlAirTableException(
                        errorMessage, 
                        SdlAirTableException.AirtableErrorType.CommunicationError);

                    Logger.Error(exception);
                    throw exception;
                }

                AirtableRecord record = createRecordResponse.Record;

                if (!playerRecord.Fields.ContainsKey("Adjustments"))
                {
                    playerRecord.Fields["Adjustments"] = new JArray();
                }

                IEnumerable<JToken> updatedAdjustmentIds = ((JArray) playerRecord.Fields["Adjustments"]).Append(record.Id);

                Fields updatePlayerFields = new Fields();
                updatePlayerFields.AddField("Adjustments", updatedAdjustmentIds.ToArray());

                Task<AirtableCreateUpdateReplaceRecordResponse> updateRecordTask =
                    airtableBase.UpdateRecord("Draft Standings", updatePlayerFields, playerRecord.Id, true);

                AirtableCreateUpdateReplaceRecordResponse updateRecordResponse = await updateRecordTask;

                if (!updateRecordResponse.Success)
                {
                    string errorMessage = updateRecordResponse.AirtableApiError != null
                        ? updateRecordResponse.AirtableApiError.ErrorMessage
                        : "Unknown error";

                    SdlAirTableException exception = new SdlAirTableException(
                        errorMessage,
                        SdlAirTableException.AirtableErrorType.CommunicationError);

                    Logger.Error(exception);
                    throw exception;
                }
            }
        }

        public static async Task<SdlPlayer[]> RetrieveAllSdlPlayers()
        {
            AirtableRecord[] records = await GetAllPlayerRecords();

            return records.Select(playerRecord =>
                {
                    SdlPlayer sdlPlayer =
                        new SdlPlayer(Convert.ToUInt64(playerRecord.Fields["DiscordID"]))
                        {
                            AirtableName = playerRecord.Fields.ContainsKey("Name") ?
                                playerRecord.Fields["Name"].ToString() :
                                string.Empty,
                            PowerLevel = Convert.ToDouble(playerRecord.Fields["Power"].ToString()),
                            SwitchFriendCode = playerRecord.Fields.ContainsKey("Friend Code")
                                ? playerRecord.Fields["Friend Code"].ToString()
                                : string.Empty,
                            AirtableId = playerRecord.Id,
                            Role = playerRecord.Fields.ContainsKey("Role") ?
                                playerRecord.Fields["Role"].ToString() :
                                string.Empty
                        };

                    try
                    {
                        if (playerRecord.Fields.ContainsKey("SZ W%"))
                        {
                            sdlPlayer.WinRates[GameMode.SplatZones] = Convert.ToDouble(playerRecord.Fields["SZ W%"]);
                        }
                    }
                    catch (Exception exception)
                    {
                        // Logger.Warn(exception);
                    }

                    try
                    {
                        if (playerRecord.Fields.ContainsKey("TC W%"))
                        {
                            sdlPlayer.WinRates[GameMode.TowerControl] = Convert.ToDouble(playerRecord.Fields["TC W%"]);
                        }
                    }
                    catch (Exception exception)
                    {
                        // Logger.Warn(exception);
                    }

                    try
                    {
                        if (playerRecord.Fields.ContainsKey("RM W%"))
                        {
                            sdlPlayer.WinRates[GameMode.Rainmaker] = Convert.ToDouble(playerRecord.Fields["RM W%"]);
                        }
                    }
                    catch (Exception exception)
                    {
                        // Logger.Warn(exception);
                    }

                    try
                    {
                        if (playerRecord.Fields.ContainsKey("CB W%"))
                        {
                            sdlPlayer.WinRates[GameMode.ClamBlitz] = Convert.ToDouble(playerRecord.Fields["CB W%"]);
                        }
                    }
                    catch (Exception exception)
                    {
                        // Logger.Warn(exception);
                    }

                    return sdlPlayer;
                })
                .ToArray();
        }

        public static async Task<SdlPlayer> RetrieveSdlPlayer(ulong discordId)
        {
            try
            {
                AirtableRecord playerRecord = await GetPlayerRecord(discordId);

                SdlPlayer sdlPlayer = new SdlPlayer(discordId)
                {
                    AirtableName = playerRecord.Fields.ContainsKey("Name") ?
                        playerRecord.Fields["Name"].ToString() :
                        string.Empty,
                    PowerLevel = Convert.ToDouble(playerRecord.Fields["Power"].ToString()),
                    SwitchFriendCode = playerRecord.Fields.ContainsKey("Friend Code") ? 
                        playerRecord.Fields["Friend Code"].ToString() : 
                        string.Empty,
                    AirtableId = playerRecord.Id,
                    Role = playerRecord.Fields.ContainsKey("Role") ?
                        playerRecord.Fields["Role"].ToString() :
                        string.Empty
                };

                try
                {
                    if (playerRecord.Fields.ContainsKey("W%"))
                    {
                        sdlPlayer.OverallWinRate = Convert.ToDouble(playerRecord.Fields["W%"]);
                    }
                    else
                    {
                        sdlPlayer.OverallWinRate = -1;
                    }
                }
                catch (Exception e)
                {
                    Logger.Warn(e);
                }

                try
                {
                    if (playerRecord.Fields.ContainsKey("SZ W%"))
                    {
                        sdlPlayer.WinRates[GameMode.SplatZones] = Convert.ToDouble(playerRecord.Fields["SZ W%"]);
                    }
                }
                catch (Exception e)
                {
                    Logger.Warn(e);
                }

                try
                {
                    if (playerRecord.Fields.ContainsKey("TC W%"))
                    {
                        sdlPlayer.WinRates[GameMode.TowerControl] = Convert.ToDouble(playerRecord.Fields["TC W%"]);
                    }
                }
                catch (Exception e)
                {
                    Logger.Warn(e);
                }

                try
                {
                    if (playerRecord.Fields.ContainsKey("RM W%"))
                    {
                        sdlPlayer.WinRates[GameMode.Rainmaker] = Convert.ToDouble(playerRecord.Fields["RM W%"]);
                    }
                }
                catch (Exception e)
                {
                    Logger.Warn(e);
                }

                try
                {
                    if (playerRecord.Fields.ContainsKey("CB W%"))
                    {
                        sdlPlayer.WinRates[GameMode.ClamBlitz] = Convert.ToDouble(playerRecord.Fields["CB W%"]);
                    }
                }
                catch (Exception e)
                {
                    Logger.Warn(e);
                }

                return sdlPlayer;
            }
            catch (Exception e)
            {
                SdlAirTableException caughtAirTableException = new SdlAirTableException(
                    e.Message, SdlAirTableException.AirtableErrorType.Generic);
                Logger.Error(caughtAirTableException);
                throw caughtAirTableException;
            }
        }

        private static async Task<AirtableRecord[]> GetAllPlayerRecords()
        {
            string offset = null;
            string errorMessage = null;
            List<AirtableRecord> records = new List<AirtableRecord>();

            using (AirtableBase airtableBase = new AirtableBase(Globals.BotSettings.AppKey, Globals.BotSettings.BaseId))
            {
                do
                {
                    Logger.Info($"Retrieving data with offset {offset}.");

                    Task<AirtableListRecordsResponse> task = airtableBase.ListRecords(
                        "Draft Standings",
                        offset,
                        null,
                        null,
                        null,
                        null
                        );

                    AirtableListRecordsResponse response = await task;

                    if (response.Success)
                    {
                        Logger.Info($"Success! Continuing with offset \"{response.Offset}\"");
                        records.AddRange(response.Records.ToList());
                        offset = response.Offset;
                    }
                    else if (response.AirtableApiError != null)
                    {
                        errorMessage = response.AirtableApiError.ErrorMessage;
                        break;
                    }
                    else
                    {
                        errorMessage = "Unknown error";
                        break;
                    }

                } while (offset != null);
            }

            if (!string.IsNullOrEmpty(errorMessage))
            {
                SdlAirTableException airTableException = new SdlAirTableException(
                    errorMessage, SdlAirTableException.AirtableErrorType.CommunicationError);
                Logger.Error(airTableException);
                throw airTableException;
            }

            return records.ToArray();
        }

        private static async Task<AirtableRecord> GetPlayerRecord(ulong discordId)
        {
            string offset = null;
            string errorMessage = null;
            List<AirtableRecord> records = new List<AirtableRecord>();

            using (AirtableBase airtableBase = new AirtableBase(Globals.BotSettings.AppKey, Globals.BotSettings.BaseId))
            {
                do
                {
                    Logger.Info($"Retrieving data with offset {offset}.");

                    Task<AirtableListRecordsResponse> task = airtableBase.ListRecords(
                        "Draft Standings",
                        offset,
                        null,
                        null,
                        null,
                        null
                        );

                    AirtableListRecordsResponse response = await task;

                    if (response.Success)
                    {
                        Logger.Info($"Success! Continuing with offset \"{response.Offset}\"");
                        records.AddRange(response.Records.ToList());
                        offset = response.Offset;
                    }
                    else if (response.AirtableApiError != null)
                    {
                        errorMessage = response.AirtableApiError.ErrorMessage;
                        break;
                    }
                    else
                    {
                        errorMessage = "Unknown error";
                        break;
                    }

                } while (offset != null);
            }

            if (!string.IsNullOrEmpty(errorMessage))
            {
                SdlAirTableException airTableException = new SdlAirTableException(
                    errorMessage, SdlAirTableException.AirtableErrorType.CommunicationError);
                Logger.Error(airTableException);
                throw airTableException;
            }

            Logger.Info("Searching for needed player id.");

            try
            {
                records = records.Where(e => e.Fields["DiscordID"].ToString() == discordId.ToString()).ToList();

                if (records.Count > 1)
                {
                    SdlAirTableException dupeAirtableException = new SdlAirTableException(
                        $"There are multiple records in the Draft Standings table with the id {discordId}!", 
                        SdlAirTableException.AirtableErrorType.UnexpectedDuplicate);
                    Logger.Error(dupeAirtableException);
                    throw dupeAirtableException;
                }

                if (records.Count == 0)
                {
                    SdlAirTableException noneAirTableException = new SdlAirTableException(
                        $"There are no players registered with the discord id {discordId}!", 
                        SdlAirTableException.AirtableErrorType.NotFound);

                    Logger.Warn(noneAirTableException);
                    throw noneAirTableException;
                }

                return records.First();
            }
            catch (Exception e)
            {
                if (!(e is SdlAirTableException sdlAirTableException))
                {
                    sdlAirTableException = new SdlAirTableException(
                        e.Message, SdlAirTableException.AirtableErrorType.Generic);
                }

                Logger.Error(sdlAirTableException);
                throw sdlAirTableException;
            }
        }
    }
}

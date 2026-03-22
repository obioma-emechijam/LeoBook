// data_repository.dart: data_repository.dart: Widget/screen for App — Repositories.
// Part of LeoBook App — Repositories
//
// Classes: DataRepository

import 'package:flutter/foundation.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:leobookapp/data/models/match_model.dart';
import 'package:leobookapp/data/models/recommendation_model.dart';
import 'package:leobookapp/data/models/standing_model.dart';
import 'package:leobookapp/data/models/league_model.dart';
import 'dart:convert';
import 'dart:async';

class DataRepository {
  static const String _keyRecommended = 'cached_recommended';
  static const String _keyPredictions = 'cached_predictions';

  final SupabaseClient _supabase = Supabase.instance.client;

  Future<List<MatchModel>> fetchMatches({DateTime? date}) async {
    try {
      // Primary source: schedules (always populated).
      // Predictions table may be empty — treat as optional enrichment.
      var query = _supabase.from('schedules').select();

      if (date != null) {
        final dateStr =
            "${date.year}-${date.month.toString().padLeft(2, '0')}-${date.day.toString().padLeft(2, '0')}";
        query = query.eq('date', dateStr);
      }

      final ordered = query.order('date', ascending: false);
      final response = date != null
          ? await ordered
          : await ordered.limit(300);

      final scheduleRows = response as List;

      // Try to enrich with predictions (fire-and-forget; don't block if empty)
      Map<String, Map<String, dynamic>> predMap = {};
      try {
        if (date != null) {
          final dateStr =
              "${date.year}-${date.month.toString().padLeft(2, '0')}-${date.day.toString().padLeft(2, '0')}";
          final predRes = await _supabase
              .from('predictions')
              .select()
              .eq('date', dateStr);
          for (var p in (predRes as List)) {
            final fid = p['fixture_id']?.toString();
            if (fid != null) predMap[fid] = p;
          }
        }
      } catch (_) {
        // predictions table empty or failed — proceed with schedules only
      }

      // Merge: schedule row + prediction overlay
      final matches = scheduleRows.map((row) {
        final fid = row['fixture_id']?.toString() ?? '';
        final pred = predMap[fid];
        // Overlay prediction fields onto schedule row
        final merged = pred != null ? {...row, ...pred} : row;
        return MatchModel.fromCsv(merged, merged);
      }).toList();

      // Cache a small subset locally
      try {
        final prefs = await SharedPreferences.getInstance();
        final listToCache = scheduleRows.take(50).toList();
        await prefs.setString(_keyPredictions, jsonEncode(listToCache));
      } catch (e) {
        debugPrint('Warning: Could not cache matches (quota exceeded): $e');
      }

      return matches;
    } catch (e) {
      debugPrint("DataRepository Error (Supabase): $e");

      // Fallback to cache
      final prefs = await SharedPreferences.getInstance();
      final cachedString = prefs.getString(_keyPredictions);

      if (cachedString != null) {
        try {
          final List<dynamic> cachedData = jsonDecode(cachedString);
          return cachedData
              .map((row) => MatchModel.fromCsv(row, row))
              .toList();
        } catch (cacheError) {
          debugPrint("Failed to load from cache: $cacheError");
        }
      }
      return [];
    }
  }

  Future<List<MatchModel>> getTeamMatches(String teamName) async {
    try {
      // Run predictions + schedules in parallel (not sequential) to halve latency
      final results = await Future.wait([
        _supabase
            .from('predictions')
            .select()
            .or('home_team.eq."$teamName",away_team.eq."$teamName"')
            .order('date', ascending: false)
            .limit(10),
        _supabase
            .from('schedules')
            .select()
            .or('home_team.eq."$teamName",away_team.eq."$teamName"')
            .order('date', ascending: false)
            .limit(10),
      ]);

      List<dynamic> predList = results[0] as List<dynamic>;
      List<dynamic> schedList = results[1] as List<dynamic>;

      // Fallback to fuzzy match only if both are empty
      if (predList.isEmpty && schedList.isEmpty) {
        final fuzzy = await Future.wait([
          _supabase
              .from('predictions')
              .select()
              .or('home_team.ilike."%$teamName%",away_team.ilike."%$teamName%"')
              .order('date', ascending: false)
              .limit(10),
          _supabase
              .from('schedules')
              .select()
              .or('home_team.ilike."%$teamName%",away_team.ilike."%$teamName%"')
              .order('date', ascending: false)
              .limit(10),
        ]);
        predList = fuzzy[0] as List<dynamic>;
        schedList = fuzzy[1] as List<dynamic>;
      }

      final List<MatchModel> matches = [];
      final Set<String> seenIds = {};

      void addMatches(List<dynamic> rows, bool isPrediction) {
        for (var row in rows) {
          final m = isPrediction
              ? MatchModel.fromCsv(row, row)
              : MatchModel.fromCsv(row);
          final id = m.fixtureId;
          if (!seenIds.contains(id)) {
            matches.add(m);
            seenIds.add(id);
          }
        }
      }

      addMatches(predList, true);
      addMatches(schedList, false);

      // Skip fetchTeamCrests() — crests already present in prediction/schedule rows.
      // The full teams table scan was causing statement timeouts on free tier.

      matches.sort((a, b) {
        try {
          return DateTime.parse(b.date).compareTo(DateTime.parse(a.date));
        } catch (_) {
          return 0;
        }
      });

      return matches;
    } catch (e) {
      debugPrint("DataRepository Error (Team Matches): $e");
      return [];
    }
  }

  Future<List<RecommendationModel>> fetchRecommendations() async {
    final prefs = await SharedPreferences.getInstance();
    try {
      final response = await _supabase
          .from('predictions')
          .select()
          .gt('recommendation_score', 0)
          .order('recommendation_score', ascending: false)
          .limit(100);

      debugPrint('Loaded ${response.length} recommendations from Supabase');

      // SharedPreferences String length limit quota can trigger on large arrays.
      // Cache only the top 30 highest scoring recommendations to avoid exceeding quota.
      final listToCache = (response as List).take(30).toList();
      try {
        await prefs.setString(_keyRecommended, jsonEncode(listToCache));
      } catch (e) {
        debugPrint('Warning: Could not save recommendations cache due to size limit: $e');
        try { await prefs.remove(_keyRecommended); } catch (_) {}
      }

      return (response)
          .map((json) => RecommendationModel.fromJson(json))
          .toList();
    } catch (e) {
      debugPrint("Error fetching recommendations (Supabase): $e");
      final cached = prefs.getString(_keyRecommended);
      if (cached != null) {
        try {
          final List<dynamic> jsonList = jsonDecode(cached);
          return jsonList
              .map((json) => RecommendationModel.fromJson(json))
              .toList();
        } catch (cacheError) {
          debugPrint("Failed to load recommendations from cache: $cacheError");
        }
      }
      return [];
    }
  }

  Future<List<StandingModel>> fetchStandings({required String leagueId, String? season}) async {
    try {
      // Safety net: if leagueId is a composite string (e.g. "REGION: League Name"), strip the region.
      // Primary fix is in UI passing leagueId, but this handles legacy or mixed cases.
      String cleanId = leagueId;
      if (cleanId.contains(': ')) {
        cleanId = cleanId.split(': ').last.trim();
      }

      var query = _supabase
          .from('computed_standings')
          .select()
          .eq('league_id', cleanId);
      
      if (season != null) {
        query = query.eq('season', season);
      }

      final response = await query
          .order('points', ascending: false)
          .order('goal_difference', ascending: false);

      // Enrich standings with team crests from teams table
      final standings =
          (response as List).map((row) => StandingModel.fromJson(row)).toList();

      if (standings.isNotEmpty) {
        try {
          final teamNames = standings.map((s) => s.teamName).toList();
          final teamsResponse = await _supabase
              .from('teams')
              .select('name, crest')
              .inFilter('name', teamNames);
          final Map<String, String> crestMap = {};
          for (var row in (teamsResponse as List)) {
            final name = row['name']?.toString();
            final crest = row['crest']?.toString();
            if (name != null &&
                crest != null &&
                crest.isNotEmpty &&
                crest != 'Unknown') {
              crestMap[name] = crest;
            }
          }
          // Merge crests into standings
          for (int i = 0; i < standings.length; i++) {
            final crest = crestMap[standings[i].teamName];
            if (crest != null && standings[i].teamCrestUrl == null) {
              standings[i] = StandingModel(
                teamName: standings[i].teamName,
                teamId: standings[i].teamId,
                teamCrestUrl: crest,
                position: standings[i].position,
                played: standings[i].played,
                wins: standings[i].wins,
                draws: standings[i].draws,
                losses: standings[i].losses,
                goalsFor: standings[i].goalsFor,
                goalsAgainst: standings[i].goalsAgainst,
                points: standings[i].points,
                leagueName: standings[i].leagueName,
              );
            }
          }
        } catch (e) {
          debugPrint("Could not fetch team crests for standings: $e");
        }
      }

      return standings;
    } catch (e) {
      debugPrint("DataRepository Error (Standings): $e");
      return [];
    }
  }

  Future<Map<String, String>> fetchTeamCrests() async {
    try {
      final response = await _supabase.from('teams').select('name, crest');
      final Map<String, String> crests = {};
      for (var row in (response as List)) {
        if (row['name'] != null && row['crest'] != null) {
          crests[row['name'].toString()] = row['crest'].toString();
        }
      }
      return crests;
    } catch (e) {
      debugPrint("DataRepository Error (Team Crests): $e");
      return {};
    }
  }

  Future<List<MatchModel>> fetchAllSchedules({DateTime? date}) async {
    try {
      // Schedules are stored in the fixtures table (not a separate table)
      var query = _supabase.from('schedules').select();

      if (date != null) {
        final dateStr =
            "${date.year}-${date.month.toString().padLeft(2, '0')}-${date.day.toString().padLeft(2, '0')}";
        query = query.eq('date', dateStr);
      }

      final ordered = query.order('date', ascending: false);
      final response = date != null
          ? await ordered
          : await ordered.limit(300);

      return (response as List).map((row) => MatchModel.fromCsv(row)).toList();
    } catch (e) {
      debugPrint("DataRepository Error (Fixtures/Schedules): $e");
      return [];
    }
  }

  Future<StandingModel?> getTeamStanding(String teamName) async {
    try {
      final response = await _supabase
          .from('computed_standings')
          .select()
          .eq('team_name', teamName)
          .maybeSingle();

      if (response != null) {
        return StandingModel.fromJson(response);
      }
      return null;
    } catch (e) {
      debugPrint("DataRepository Error (Team Standing): $e");
      return null;
    }
  }

  // --- Realtime Streams (Postgres Changes Style) ---

  Stream<List<MatchModel>> watchLiveScores() {
    return _supabase.from('live_scores').stream(primaryKey: ['fixture_id']).map(
        (rows) => rows.map((row) => MatchModel.fromCsv(row)).toList());
  }

  Stream<List<MatchModel>> watchPredictions({DateTime? date}) {
    var query =
        _supabase.from('predictions').stream(primaryKey: ['fixture_id']);

    return query.map((rows) {
      var matches = rows.map((row) => MatchModel.fromCsv(row, row)).toList();
      if (date != null) {
        final dateStr =
            "${date.year}-${date.month.toString().padLeft(2, '0')}-${date.day.toString().padLeft(2, '0')}";
        matches = matches.where((m) => m.date == dateStr).toList();
      }
      return matches;
    });
  }

  Stream<List<MatchModel>> watchSchedules({DateTime? date}) {
    // Schedules are stored in the fixtures table
    var query = _supabase.from('schedules').stream(primaryKey: ['fixture_id']);

    return query.map((rows) {
      var matches = rows.map((row) => MatchModel.fromCsv(row)).toList();
      if (date != null) {
        final dateStr =
            "${date.year}-${date.month.toString().padLeft(2, '0')}-${date.day.toString().padLeft(2, '0')}";
        matches = matches.where((m) => m.date == dateStr).toList();
      }
      return matches;
    });
  }

  Stream<List<StandingModel>> watchStandings({required String leagueId, String? season}) {
    final controller = StreamController<List<StandingModel>>.broadcast();

    void fetchAndEmit() async {
      try {
        final data = await fetchStandings(leagueId: leagueId, season: season);
        if (!controller.isClosed) controller.add(data);
      } catch (e) {
        debugPrint('Error fetching standings view: $e');
      }
    }

    // Initial fetch
    fetchAndEmit();

    // Listen to underlying schedules table for changes since computed_standings view doesn't emit realtime events natively
    final channel = _supabase.channel('schedules:standings-$leagueId');
    channel.onPostgresChanges(
      event: PostgresChangeEvent.all,
      schema: 'public',
      table: 'schedules',
      filter: PostgresChangeFilter(
        type: PostgresChangeFilterType.eq,
        column: 'league_id',
        value: leagueId,
      ),
      callback: (payload) {
        fetchAndEmit();
      },
    ).subscribe();

    controller.onCancel = () {
      _supabase.removeChannel(channel);
    };

    return controller.stream;
  }


  Stream<Map<String, String>> watchTeamCrestUpdates() {
    return _supabase.from('teams').stream(primaryKey: ['name']).map((rows) {
      final Map<String, String> crests = {};
      for (var row in rows) {
        if (row['name'] != null && row['crest'] != null) {
          crests[row['name'].toString()] = row['crest'].toString();
        }
      }
      return crests;
    });
  }

  final Map<String, List<Map<String, dynamic>>> _oddsCache = {};

  Future<List<Map<String, dynamic>>> getMatchOdds(String fixtureId) async {
    if (_oddsCache.containsKey(fixtureId)) return _oddsCache[fixtureId]!;
    try {
      final response = await _supabase
          .from('match_odds')
          .select()
          .eq('fixture_id', fixtureId);
      final list = List<Map<String, dynamic>>.from(response);
      _oddsCache[fixtureId] = list;
      return list;
    } catch (e) {
      debugPrint('Error fetching odds for $fixtureId: $e');
      return [];
    }
  }

  /// Watch match_odds table for realtime odds updates
  Stream<List<Map<String, dynamic>>> watchMatchOdds(String fixtureId) {
    return _supabase
        .from('match_odds')
        .stream(primaryKey: ['fixture_id', 'market_id', 'exact_outcome'])
        .eq('fixture_id', fixtureId)
        .map((rows) => List<Map<String, dynamic>>.from(rows));
  }

  // --- League Data ---

  Future<List<LeagueModel>> fetchLeagues() async {
    try {
      final response = await _supabase
          .from('leagues')
          .select(
              'league_id, fs_league_id, name, crest, continent, region, region_flag, current_season, country_code, url')
          .order('name', ascending: true);

      return (response as List)
          .map((row) => LeagueModel.fromJson(row))
          .toList();
    } catch (e) {
      debugPrint("DataRepository Error (Leagues): $e");
      return [];
    }
  }

  Future<LeagueModel?> fetchLeagueById(String leagueId) async {
    try {
      final response = await _supabase
          .from('leagues')
          .select()
          .eq('league_id', leagueId)
          .maybeSingle();

      if (response != null) {
        // Null-safe accessor for 'region' to handle Supabase schema drift (missing column)
        final data = Map<String, dynamic>.from(response);
        if (!data.containsKey('region')) {
          data['region'] = '';
        }
        return LeagueModel.fromJson(data);
      }
      return null;
    } catch (e) {
      debugPrint("DataRepository Error (League by ID): $e");
      return null;
    }
  }

  Future<List<MatchModel>> fetchFixturesByLeague(String leagueId,
      {String? season}) async {
    try {
      var query =
          _supabase.from('schedules').select().eq('league_id', leagueId);

      if (season != null) {
        query = query.eq('season', season);
      }

      final response = await query.order('date', ascending: false).limit(500);

      return (response as List).map((row) => MatchModel.fromCsv(row)).toList();
    } catch (e) {
      debugPrint("DataRepository Error (Fixtures by League): $e");
      return [];
    }
  }
  Future<List<String>> fetchLeagueSeasons(String leagueId) async {
    try {
      // Fetch distinct seasons from schedules for this league
      final response = await _supabase
          .from('schedules')
          .select('season')
          .eq('league_id', leagueId)
          .not('season', 'is', null)
          .order('season', ascending: false);

      final Set<String> seasons = {};
      for (var row in (response as List)) {
        final s = row['season']?.toString() ?? '';
        if (s.isNotEmpty) seasons.add(s);
      }
      return seasons.toList()..sort((a, b) => b.compareTo(a));
    } catch (e) {
      debugPrint("DataRepository Error (League Seasons): $e");
      return [];
    }
  }
}

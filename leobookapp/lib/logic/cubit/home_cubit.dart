// home_cubit.dart: home_cubit.dart: Widget/screen for App — State Management (Cubit).
// Part of LeoBook App — State Management (Cubit)
//
// Classes: HomeState, HomeInitial, HomeLoading, HomeLoaded, HomeError, HomeCubit

import 'dart:async';
import 'package:flutter/foundation.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:leobookapp/data/models/match_model.dart';
import 'package:leobookapp/data/repositories/data_repository.dart';

import 'package:leobookapp/data/models/news_model.dart';
import 'package:leobookapp/data/models/recommendation_model.dart';
import 'package:leobookapp/data/repositories/news_repository.dart';

// States
abstract class HomeState {}

class HomeInitial extends HomeState {}

class HomeLoading extends HomeState {}

class HomeLoaded extends HomeState {
  final List<MatchModel> allMatches;
  final List<MatchModel> filteredMatches;
  final List<MatchModel> featuredMatches;
  final List<MatchModel> liveMatches;
  final List<NewsModel> news;
  final List<RecommendationModel> allRecommendations;
  final List<RecommendationModel> filteredRecommendations;
  final DateTime selectedDate;
  final String selectedSport;
  final List<String> availableSports;
  final bool isAllMatchesExpanded;

  // Advanced Filters
  final List<String> selectedLeagues;
  final List<String> selectedPredictionTypes;
  final double minOdds;
  final double maxOdds;
  final double minReliability;
  final List<String> selectedConfidenceLevels;
  final bool onlyAvailable;

  // Available Filter Options
  final List<String> availableLeagues;
  final List<String> availablePredictionTypes;

  HomeLoaded({
    required this.allMatches,
    required this.filteredMatches,
    required this.featuredMatches,
    this.liveMatches = const [],
    this.news = const [],
    required this.allRecommendations,
    required this.filteredRecommendations,
    required this.selectedDate,
    this.selectedSport = 'ALL',
    this.availableSports = const ['ALL'],
    this.isAllMatchesExpanded = false,
    this.selectedLeagues = const [],
    this.selectedPredictionTypes = const [],
    this.minOdds = 1.0,
    this.maxOdds = 10.0,
    this.minReliability = 0.0,
    this.selectedConfidenceLevels = const [],
    this.onlyAvailable = false,
    this.availableLeagues = const [],
    this.availablePredictionTypes = const [],
  });
}

class HomeError extends HomeState {
  final String message;
  HomeError(this.message);
}

class HomeCubit extends Cubit<HomeState> {
  final DataRepository _dataRepository;
  final NewsRepository _newsRepository;
  StreamSubscription? _predictionsSub;
  StreamSubscription? _schedulesSub;
  StreamSubscription? _teamCrestsSub;
  Timer? _refreshTimer;
  bool _isRefreshing = false;

  HomeCubit(this._dataRepository, this._newsRepository) : super(HomeInitial());

  Future<void> loadDashboard() async {
    emit(HomeLoading());
    try {
      final now = DateTime.now();

      // Fetch news once
      final news = await _newsRepository.fetchNews();

      // Try fetching predictions for today
      List<MatchModel> matches = await _dataRepository.fetchMatches(date: now);
      List<RecommendationModel> recommendations =
          await _dataRepository.fetchRecommendations();

      DateTime selectionDate = now;

      // If no matches for today, find the most recent date with predictions
      if (matches.isEmpty) {
        final allRecent =
            await _dataRepository.fetchMatches(); // Get latest 200
        if (allRecent.isNotEmpty) {
          final latestDateStr = allRecent
              .map((m) => m.date)
              .reduce((a, b) => a.compareTo(b) > 0 ? a : b);
          selectionDate = DateTime.parse(latestDateStr);
          matches = allRecent.where((m) => m.date == latestDateStr).toList();
        }
      }

      const defaultSport = 'ALL';
      final filteredRecs = _filterRecommendations(
        recommendations,
        selectionDate,
        defaultSport,
      );

      final live = matches.where((m) => m.isLive).toList();

      // Featured logic
      final featured = matches
          .where((m) => m.confidence != null && m.confidence!.contains('High'))
          .toList();

      final sportsSet = {'ALL'};
      final leaguesSet = <String>{};
      final typesSet = <String>{};

      for (var m in matches) {
        sportsSet.add(m.sport.toUpperCase());
        if (m.league != null) leaguesSet.add(m.league!);
        if (m.prediction != null) typesSet.add(m.prediction!);
      }
      for (var r in recommendations) {
        sportsSet.add(r.sport.toUpperCase());
        leaguesSet.add(r.league);
        typesSet.add(r.prediction);
      }
      final availableSports = sportsSet.toList()..sort();
      final availableLeagues = leaguesSet.toList()..sort();
      final availablePredictionTypes = typesSet.toList()..sort();

      emit(
        HomeLoaded(
          allMatches: matches,
          filteredMatches: matches,
          featuredMatches: featured,
          liveMatches: live,
          news: news,
          allRecommendations: recommendations,
          filteredRecommendations: filteredRecs,
          selectedDate: selectionDate,
          selectedSport: defaultSport,
          availableSports: availableSports,
          isAllMatchesExpanded: false,
          availableLeagues: availableLeagues,
          availablePredictionTypes: availablePredictionTypes,
        ),
      );

      // --- Start Realtime Subscriptions (skip on Web — broken realtime_client) ---
      _predictionsSub?.cancel();
      _liveScoresSub?.cancel();
      _schedulesSub?.cancel();
      _teamCrestsSub?.cancel();
      _predictionsSub = null;
      _liveScoresSub = null;
      _schedulesSub = null;
      _teamCrestsSub = null;

      if (!kIsWeb) {
        _predictionsSub = _dataRepository
            .watchPredictions(date: selectionDate)
            .listen((updatedMatches) {
          _handleRealtimeUpdate(updatedMatches);
        }, onError: (e) {
          debugPrint("Predictions Stream Error: $e");
        });

        _liveScoresSub =
            _dataRepository.watchLiveScores().listen((liveUpdates) {
          _handleRealtimeUpdate(liveUpdates);
        }, onError: (e) {
          debugPrint("LiveScores Stream Error: $e");
        });

        _schedulesSub = _dataRepository
            .watchSchedules(date: selectionDate)
            .listen((scheduleUpdates) {
          _handleRealtimeUpdate(scheduleUpdates);
        }, onError: (e) {
          debugPrint("Schedules Stream Error: $e");
        });

        _teamCrestsSub =
            _dataRepository.watchTeamCrestUpdates().listen((crestMap) {
          _handleCrestUpdate(crestMap);
        }, onError: (e) {
          debugPrint("TeamCrests Stream Error: $e");
        });
      }

      // --- Start 3-second periodic refresh (upsert-only, skips if no changes) ---
      _refreshTimer?.cancel();
      _refreshTimer = Timer.periodic(
        const Duration(seconds: 3),
        (_) => _periodicRefresh(),
      );
    } catch (e) {
      emit(HomeError("Failed to load dashboard: $e"));
    }
  }

  StreamSubscription? _liveScoresSub;

  /// Periodic background refresh — fetches latest data and upserts only changed matches.
  Future<void> _periodicRefresh() async {
    if (isClosed || _isRefreshing || state is! HomeLoaded) return;
    _isRefreshing = true;
    try {
      final currentState = state as HomeLoaded;
      final freshMatches = await _dataRepository.fetchMatches(date: currentState.selectedDate);

      // Guard: if we have good data and fresh fetch is suspiciously small,
      // it's likely a transient Supabase timeout — skip this refresh cycle.
      if (freshMatches.isEmpty || isClosed) return;
      if (currentState.allMatches.length > 10 &&
          freshMatches.length < currentState.allMatches.length * 0.3) {
        debugPrint('Periodic refresh: skipping suspicious result '
            '(${freshMatches.length} vs ${currentState.allMatches.length})');
        return;
      }

      // Upsert: only merge if data actually changed
      final Map<String, MatchModel> matchMap = {
        for (var m in currentState.allMatches) m.fixtureId: m,
      };

      bool anyChanged = false;
      for (var fresh in freshMatches) {
        final existing = matchMap[fresh.fixtureId];
        if (existing == null) {
          matchMap[fresh.fixtureId] = fresh;
          anyChanged = true;
        } else {
          // Compare key volatile fields
          if (existing.status != fresh.status ||
              existing.homeScore != fresh.homeScore ||
              existing.awayScore != fresh.awayScore ||
              existing.liveMinute != fresh.liveMinute ||
              existing.odds != fresh.odds ||
              existing.prediction != fresh.prediction) {
            matchMap[fresh.fixtureId] = existing.mergeWith(fresh);
            anyChanged = true;
          }
        }
      }

      if (!anyChanged || isClosed) return;

      final mergedMatches = matchMap.values.toList();
      final filteredMatches = _filterMatches(
        mergedMatches,
        currentState.selectedDate,
        currentState.selectedSport,
        leagues: currentState.selectedLeagues,
        types: currentState.selectedPredictionTypes,
        minO: currentState.minOdds,
        maxO: currentState.maxOdds,
        minRel: currentState.minReliability,
        confs: currentState.selectedConfidenceLevels,
        onlyAvail: currentState.onlyAvailable,
      );

      emit(HomeLoaded(
        allMatches: mergedMatches,
        filteredMatches: filteredMatches,
        featuredMatches: mergedMatches
            .where((m) => m.confidence != null && m.confidence!.contains('High'))
            .toList(),
        liveMatches: mergedMatches.where((m) => m.isLive).toList(),
        news: currentState.news,
        allRecommendations: currentState.allRecommendations,
        filteredRecommendations: currentState.filteredRecommendations,
        selectedDate: currentState.selectedDate,
        selectedSport: currentState.selectedSport,
        availableSports: currentState.availableSports,
        isAllMatchesExpanded: currentState.isAllMatchesExpanded,
        selectedLeagues: currentState.selectedLeagues,
        selectedPredictionTypes: currentState.selectedPredictionTypes,
        minOdds: currentState.minOdds,
        maxOdds: currentState.maxOdds,
        minReliability: currentState.minReliability,
        selectedConfidenceLevels: currentState.selectedConfidenceLevels,
        onlyAvailable: currentState.onlyAvailable,
        availableLeagues: currentState.availableLeagues,
        availablePredictionTypes: currentState.availablePredictionTypes,
      ));
    } catch (e) {
      debugPrint('Periodic refresh error: $e');
    } finally {
      _isRefreshing = false;
    }
  }

  void _handleRealtimeUpdate(List<MatchModel> updatedMatches) {
    if (state is HomeLoaded) {
      final currentState = state as HomeLoaded;

      // Merge updated matches into the current state preservation prediction data
      final Map<String, MatchModel> matchMap = {
        for (var m in currentState.allMatches) m.fixtureId: m,
      };

      for (var updated in updatedMatches) {
        final existing = matchMap[updated.fixtureId];
        if (existing != null) {
          matchMap[updated.fixtureId] = existing.mergeWith(updated);
        } else {
          matchMap[updated.fixtureId] = updated;
        }
      }

      final mergedMatches = matchMap.values.toList();

      final filteredMatches = _filterMatches(
        mergedMatches,
        currentState.selectedDate,
        currentState.selectedSport,
        leagues: currentState.selectedLeagues,
        types: currentState.selectedPredictionTypes,
        minO: currentState.minOdds,
        maxO: currentState.maxOdds,
        minRel: currentState.minReliability,
        confs: currentState.selectedConfidenceLevels,
        onlyAvail: currentState.onlyAvailable,
      );

      final live = mergedMatches.where((m) => m.isLive).toList();
      final featured = mergedMatches
          .where((m) => m.confidence != null && m.confidence!.contains('High'))
          .toList();

      emit(
        HomeLoaded(
          allMatches: mergedMatches,
          filteredMatches: filteredMatches,
          featuredMatches: featured,
          liveMatches: live,
          news: currentState.news,
          allRecommendations: currentState.allRecommendations,
          filteredRecommendations: currentState.filteredRecommendations,
          selectedDate: currentState.selectedDate,
          selectedSport: currentState.selectedSport,
          availableSports: currentState.availableSports,
          isAllMatchesExpanded: currentState.isAllMatchesExpanded,
          selectedLeagues: currentState.selectedLeagues,
          selectedPredictionTypes: currentState.selectedPredictionTypes,
          minOdds: currentState.minOdds,
          maxOdds: currentState.maxOdds,
          minReliability: currentState.minReliability,
          selectedConfidenceLevels: currentState.selectedConfidenceLevels,
          onlyAvailable: currentState.onlyAvailable,
          availableLeagues: currentState.availableLeagues,
          availablePredictionTypes: currentState.availablePredictionTypes,
        ),
      );
    }
  }

  void updateDate(DateTime date) async {
    if (state is HomeLoaded) {
      final currentState = state as HomeLoaded;

      // Optionally show a mini-loading state here if desired,
      // but for now we'll just fetch and update.

      final matches = await _dataRepository.fetchMatches(date: date);

      final filteredMatches = _filterMatches(
        matches,
        date,
        currentState.selectedSport,
        leagues: currentState.selectedLeagues,
        types: currentState.selectedPredictionTypes,
        minO: currentState.minOdds,
        maxO: currentState.maxOdds,
        minRel: currentState.minReliability,
        confs: currentState.selectedConfidenceLevels,
        onlyAvail: currentState.onlyAvailable,
      );

      final filteredRecs = _filterRecommendations(
        currentState.allRecommendations,
        date,
        currentState.selectedSport,
        leagues: currentState.selectedLeagues,
        types: currentState.selectedPredictionTypes,
        minO: currentState.minOdds,
        maxO: currentState.maxOdds,
        minRel: currentState.minReliability,
        confs: currentState.selectedConfidenceLevels,
        onlyAvail: currentState.onlyAvailable,
      );

      final featured = filteredMatches
          .where((m) => m.confidence != null && m.confidence!.contains('High'))
          .toList();

      final sportsSet = {'ALL'};
      final leaguesSet = <String>{};
      final typesSet = <String>{};

      for (var m in matches) {
        sportsSet.add(m.sport.toUpperCase());
        if (m.league != null) leaguesSet.add(m.league!);
        if (m.prediction != null) typesSet.add(m.prediction!);
      }
      for (var r in currentState.allRecommendations) {
        sportsSet.add(r.sport.toUpperCase());
        leaguesSet.add(r.league);
        typesSet.add(r.prediction);
      }
      final availableSports = sportsSet.toList()..sort();
      final availableLeagues = leaguesSet.toList()..sort();
      final availablePredictionTypes = typesSet.toList()..sort();

      emit(
        HomeLoaded(
          allMatches: matches,
          filteredMatches: filteredMatches,
          featuredMatches: featured,
          liveMatches: matches.where((m) => m.isLive).toList(),
          news: currentState.news,
          allRecommendations: currentState.allRecommendations,
          filteredRecommendations: filteredRecs,
          selectedDate: date,
          selectedSport: currentState.selectedSport,
          availableSports: availableSports,
          isAllMatchesExpanded: false,
          selectedLeagues: currentState.selectedLeagues,
          selectedPredictionTypes: currentState.selectedPredictionTypes,
          minOdds: currentState.minOdds,
          maxOdds: currentState.maxOdds,
          minReliability: currentState.minReliability,
          selectedConfidenceLevels: currentState.selectedConfidenceLevels,
          onlyAvailable: currentState.onlyAvailable,
          availableLeagues: availableLeagues,
          availablePredictionTypes: availablePredictionTypes,
        ),
      );

      // Re-subscribe for the new date (skip on Web)
      if (!kIsWeb) {
        _predictionsSub?.cancel();
        _schedulesSub?.cancel();
        _predictionsSub = null;
        _schedulesSub = null;

        // Delay slightly to allow the previous channel to leave cleanly
        await Future.delayed(const Duration(milliseconds: 300));

        if (isClosed) return;

        _predictionsSub = _dataRepository
            .watchPredictions(date: date)
            .listen((updatedMatches) {
          _handleRealtimeUpdate(updatedMatches);
        }, onError: (e) {
          debugPrint("Predictions Stream (Update) Error: $e");
        });

        _schedulesSub = _dataRepository
            .watchSchedules(date: date)
            .listen((scheduleUpdates) {
          _handleRealtimeUpdate(scheduleUpdates);
        }, onError: (e) {
          debugPrint("Schedules Stream (Update) Error: $e");
        });
      }
    }
  }

  void updateSport(String sport) {
    if (state is HomeLoaded) {
      final currentState = state as HomeLoaded;

      final filteredMatches = _filterMatches(
        currentState.allMatches,
        currentState.selectedDate,
        sport,
        leagues: currentState.selectedLeagues,
        types: currentState.selectedPredictionTypes,
        minO: currentState.minOdds,
        maxO: currentState.maxOdds,
        minRel: currentState.minReliability,
        confs: currentState.selectedConfidenceLevels,
        onlyAvail: currentState.onlyAvailable,
      );
      final filteredRecs = _filterRecommendations(
        currentState.allRecommendations,
        currentState.selectedDate,
        sport,
        leagues: currentState.selectedLeagues,
        types: currentState.selectedPredictionTypes,
        minO: currentState.minOdds,
        maxO: currentState.maxOdds,
        minRel: currentState.minReliability,
        confs: currentState.selectedConfidenceLevels,
        onlyAvail: currentState.onlyAvailable,
      );

      final featured = filteredMatches
          .where((m) => m.confidence != null && m.confidence!.contains('High'))
          .toList();

      emit(
        HomeLoaded(
          allMatches: currentState.allMatches,
          filteredMatches: filteredMatches,
          featuredMatches: featured,
          liveMatches: currentState.liveMatches,
          news: currentState.news,
          allRecommendations: currentState.allRecommendations,
          filteredRecommendations: filteredRecs,
          selectedDate: currentState.selectedDate,
          selectedSport: sport,
          availableSports: currentState.availableSports,
          isAllMatchesExpanded: false,
          selectedLeagues: currentState.selectedLeagues,
          selectedPredictionTypes: currentState.selectedPredictionTypes,
          minOdds: currentState.minOdds,
          maxOdds: currentState.maxOdds,
          minReliability: currentState.minReliability,
          selectedConfidenceLevels: currentState.selectedConfidenceLevels,
          onlyAvailable: currentState.onlyAvailable,
          availableLeagues: currentState.availableLeagues,
          availablePredictionTypes: currentState.availablePredictionTypes,
        ),
      );
    }
  }

  void applyFilters({
    required List<String> leagues,
    required List<String> types,
    required double minOdds,
    required double maxOdds,
    required double minReliability,
    required List<String> confidenceLevels,
    required bool onlyAvailable,
  }) {
    if (state is HomeLoaded) {
      final currentState = state as HomeLoaded;

      final filteredMatches = _filterMatches(
        currentState.allMatches,
        currentState.selectedDate,
        currentState.selectedSport,
        minO: minOdds,
        maxO: maxOdds,
        minRel: minReliability,
        confs: confidenceLevels,
        onlyAvail: onlyAvailable,
      );
      final filteredRecs = _filterRecommendations(
        currentState.allRecommendations,
        currentState.selectedDate,
        currentState.selectedSport,
        leagues: leagues,
        types: types,
        minO: minOdds,
        maxO: maxOdds,
        minRel: minReliability,
        confs: confidenceLevels,
        onlyAvail: onlyAvailable,
      );

      final featured = filteredMatches
          .where((m) => m.confidence != null && m.confidence!.contains('High'))
          .toList();

      emit(
        HomeLoaded(
          allMatches: currentState.allMatches,
          filteredMatches: filteredMatches,
          featuredMatches: featured,
          liveMatches: currentState.liveMatches,
          news: currentState.news,
          allRecommendations: currentState.allRecommendations,
          filteredRecommendations: filteredRecs,
          selectedDate: currentState.selectedDate,
          selectedSport: currentState.selectedSport,
          availableSports: currentState.availableSports,
          isAllMatchesExpanded: currentState.isAllMatchesExpanded,
          selectedLeagues: leagues,
          selectedPredictionTypes: types,
          minOdds: minOdds,
          maxOdds: maxOdds,
          minReliability: minReliability,
          selectedConfidenceLevels: confidenceLevels,
          onlyAvailable: onlyAvailable,
          availableLeagues: currentState.availableLeagues,
          availablePredictionTypes: currentState.availablePredictionTypes,
        ),
      );
    }
  }

  void resetFilters() {
    if (state is HomeLoaded) {
      final currentState = state as HomeLoaded;
      updateSport(
        currentState.selectedSport,
      ); // This will effectively reset if we pass empty filters
    }
  }

  void toggleAllMatchesExpansion() {
    if (state is HomeLoaded) {
      final currentState = state as HomeLoaded;
      emit(
        HomeLoaded(
          allMatches: currentState.allMatches,
          filteredMatches: currentState.filteredMatches,
          featuredMatches: currentState.featuredMatches,
          liveMatches: currentState.liveMatches,
          news: currentState.news,
          allRecommendations: currentState.allRecommendations,
          filteredRecommendations: currentState.filteredRecommendations,
          selectedDate: currentState.selectedDate,
          selectedSport: currentState.selectedSport,
          availableSports: currentState.availableSports,
          isAllMatchesExpanded: !currentState.isAllMatchesExpanded,
          selectedLeagues: currentState.selectedLeagues,
          selectedPredictionTypes: currentState.selectedPredictionTypes,
          minOdds: currentState.minOdds,
          maxOdds: currentState.maxOdds,
          minReliability: currentState.minReliability,
          selectedConfidenceLevels: currentState.selectedConfidenceLevels,
          onlyAvailable: currentState.onlyAvailable,
          availableLeagues: currentState.availableLeagues,
          availablePredictionTypes: currentState.availablePredictionTypes,
        ),
      );
    }
  }

  List<MatchModel> _filterMatches(
    List<MatchModel> matches,
    DateTime date,
    String sport, {
    List<String> leagues = const [],
    List<String> types = const [],
    double minO = 1.0,
    double maxO = 10.0,
    double minRel = 0.0,
    List<String> confs = const [],
    bool onlyAvail = false,
  }) {
    final targetDateStr = _formatDateForMatching(date);
    return matches.where((m) {
      final dateMatch = m.date == targetDateStr;
      final sportMatch =
          (sport == 'ALL') || (m.sport.toUpperCase() == sport.toUpperCase());

      bool leagueMatch =
          leagues.isEmpty || (m.league != null && leagues.contains(m.league));
      bool typeMatch = types.isEmpty ||
          (m.prediction != null && types.any((t) => m.prediction!.contains(t)));

      double mOdds = double.tryParse(m.odds ?? '1.0') ?? 1.0;
      bool oddsMatch = mOdds >= minO && mOdds <= maxO;

      double rel = double.tryParse(m.marketReliability ?? '0.0') ?? 0.0;
      bool relMatch = rel >= minRel;

      bool confMatch = confs.isEmpty ||
          (m.confidence != null && confs.contains(m.confidence));

      bool availMatch = !onlyAvail || m.isAvailableInBookie;

      return dateMatch &&
          sportMatch &&
          leagueMatch &&
          typeMatch &&
          oddsMatch &&
          relMatch &&
          confMatch &&
          availMatch;
    }).toList();
  }

  List<RecommendationModel> _filterRecommendations(
    List<RecommendationModel> recs,
    DateTime date,
    String sport, {
    List<String> leagues = const [],
    List<String> types = const [],
    double minO = 1.0,
    double maxO = 10.0,
    double minRel = 0.0,
    List<String> confs = const [],
    bool onlyAvail = false,
  }) {
    // NOTE: No date filter — recommendations are ranked globally by score,
    // not tied to a specific day. The query fetches top 100 by recommendation_score.
    return recs.where((r) {
      final sportMatch =
          (sport == 'ALL') || (r.sport.toUpperCase() == sport.toUpperCase());

      bool leagueMatch = leagues.isEmpty || leagues.contains(r.league);
      bool typeMatch =
          types.isEmpty || types.any((t) => r.prediction.contains(t));

      double rOdds = r.marketOdds;
      bool oddsMatch = rOdds >= minO && rOdds <= maxO;

      bool relMatch = r.reliabilityScore >= minRel;
      bool confMatch = confs.isEmpty || confs.contains(r.confidence);
      bool availMatch = !onlyAvail || r.isAvailable;

      return sportMatch &&
          leagueMatch &&
          typeMatch &&
          oddsMatch &&
          relMatch &&
          confMatch &&
          availMatch;
    }).toList();
  }

  String _formatDateForMatching(DateTime date) {
    return "${date.year}-${date.month.toString().padLeft(2, '0')}-${date.day.toString().padLeft(2, '0')}";
  }

  void _handleCrestUpdate(Map<String, String> crestMap) {
    if (state is HomeLoaded && crestMap.isNotEmpty) {
      final currentState = state as HomeLoaded;
      final updatedMatches = currentState.allMatches.map((m) {
        final homeCrest = crestMap[m.homeTeam] ?? m.homeCrestUrl;
        final awayCrest = crestMap[m.awayTeam] ?? m.awayCrestUrl;
        if (homeCrest != m.homeCrestUrl || awayCrest != m.awayCrestUrl) {
          return MatchModel(
            fixtureId: m.fixtureId,
            date: m.date,
            time: m.time,
            homeTeam: m.homeTeam,
            awayTeam: m.awayTeam,
            homeTeamId: m.homeTeamId,
            awayTeamId: m.awayTeamId,
            homeScore: m.homeScore,
            awayScore: m.awayScore,
            status: m.status,
            sport: m.sport,
            league: m.league,
            prediction: m.prediction,
            odds: m.odds,
            confidence: m.confidence,
            liveMinute: m.liveMinute,
            isFeatured: m.isFeatured,
            valueTag: m.valueTag,
            homeCrestUrl: homeCrest,
            awayCrestUrl: awayCrest,
            regionFlagUrl: m.regionFlagUrl,
            marketReliability: m.marketReliability,
            xgHome: m.xgHome,
            xgAway: m.xgAway,
            reasonTags: m.reasonTags,
            homeFormN: m.homeFormN,
            awayFormN: m.awayFormN,
            outcomeCorrect: m.outcomeCorrect,
          );
        }
        return m;
      }).toList();

      emit(
        HomeLoaded(
          allMatches: updatedMatches,
          filteredMatches: _filterMatches(
            updatedMatches,
            currentState.selectedDate,
            currentState.selectedSport,
            leagues: currentState.selectedLeagues,
            types: currentState.selectedPredictionTypes,
            minO: currentState.minOdds,
            maxO: currentState.maxOdds,
            minRel: currentState.minReliability,
            confs: currentState.selectedConfidenceLevels,
            onlyAvail: currentState.onlyAvailable,
          ),
          featuredMatches: updatedMatches
              .where(
                  (m) => m.confidence != null && m.confidence!.contains('High'))
              .toList(),
          liveMatches: updatedMatches.where((m) => m.isLive).toList(),
          news: currentState.news,
          allRecommendations: currentState.allRecommendations,
          filteredRecommendations: currentState.filteredRecommendations,
          selectedDate: currentState.selectedDate,
          selectedSport: currentState.selectedSport,
          availableSports: currentState.availableSports,
          isAllMatchesExpanded: currentState.isAllMatchesExpanded,
          selectedLeagues: currentState.selectedLeagues,
          selectedPredictionTypes: currentState.selectedPredictionTypes,
          minOdds: currentState.minOdds,
          maxOdds: currentState.maxOdds,
          minReliability: currentState.minReliability,
          selectedConfidenceLevels: currentState.selectedConfidenceLevels,
          onlyAvailable: currentState.onlyAvailable,
          availableLeagues: currentState.availableLeagues,
          availablePredictionTypes: currentState.availablePredictionTypes,
        ),
      );
    }
  }

  @override
  Future<void> close() async {
    _refreshTimer?.cancel();
    _refreshTimer = null;
    for (final sub in [
      _predictionsSub,
      _liveScoresSub,
      _schedulesSub,
      _teamCrestsSub
    ]) {
      try {
        await sub?.cancel();
      } catch (e) {
        debugPrint("Error canceling subscription: $e");
      }
    }
    _predictionsSub = null;
    _liveScoresSub = null;
    _schedulesSub = null;
    _teamCrestsSub = null;
    return super.close();
  }
}

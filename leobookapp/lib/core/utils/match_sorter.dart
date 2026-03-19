// match_sorter.dart: match_sorter.dart: Widget/screen for App — Utilities.
// Part of LeoBook App — Utilities
//
// Classes: MatchSorter, MatchGroupHeader

import 'package:leobookapp/data/models/match_model.dart';

enum MatchTabType { all, live, finished, scheduled }

class MatchSorter {
  static List<dynamic> getSortedMatches(
    List<MatchModel> matches,
    MatchTabType type,
  ) {
    switch (type) {
      case MatchTabType.all:
        return _groupByLeague(matches);
      case MatchTabType.live:
        return _groupByLeague(_filterLiveMatches(matches));
      case MatchTabType.finished:
        return _groupByTime(_filterFinishedMatches(matches), descending: true);
      case MatchTabType.scheduled:
        return _groupByTime(_filterScheduledMatches(matches),
            descending: false);
    }
  }

  static List<dynamic> _groupByLeague(List<MatchModel> matches) {
    if (matches.isEmpty) return [];

    final Map<String, List<MatchModel>> groups = {};
    for (var match in matches) {
      final key = match.league?.trim() ?? "Other";
      if (!groups.containsKey(key)) {
        groups[key] = [];
      }
      groups[key]!.add(match);
    }

    final sortedKeys = groups.keys.toList()..sort();

    final List<dynamic> result = [];
    for (var key in sortedKeys) {
      final groupMatches = groups[key]!;
      groupMatches.sort((a, b) {
        int timeComp = a.time.compareTo(b.time);
        if (timeComp != 0) return timeComp;
        return a.homeTeam.compareTo(b.homeTeam);
      });

      result.add(MatchGroupHeader(title: key));
      result.addAll(groupMatches);
    }
    return result;
  }

  static List<dynamic> _groupByTime(List<MatchModel> matches,
      {required bool descending}) {
    if (matches.isEmpty) return [];

    // Group by Hour (HH:00)
    final Map<String, List<MatchModel>> groups = {};
    for (var match in matches) {
      // Assuming match.time is "HH:mm"
      final hour = match.time.split(':')[0];
      final key = "$hour:00";
      if (!groups.containsKey(key)) {
        groups[key] = [];
      }
      groups[key]!.add(match);
    }

    final sortedKeys = groups.keys.toList();
    if (descending) {
      sortedKeys.sort((a, b) => b.compareTo(a));
    } else {
      sortedKeys.sort((a, b) => a.compareTo(b));
    }

    final List<dynamic> result = [];
    for (var key in sortedKeys) {
      final groupMatches = groups[key]!;
      groupMatches.sort((a, b) {
        if (descending) {
          int timeComp = b.time.compareTo(a.time);
          if (timeComp != 0) return timeComp;
          return a.homeTeam.compareTo(b.homeTeam);
        } else {
          int timeComp = a.time.compareTo(b.time);
          if (timeComp != 0) return timeComp;
          return a.homeTeam.compareTo(b.homeTeam);
        }
      });

      result.add(MatchGroupHeader(title: key));
      result.addAll(groupMatches);
    }
    return result;
  }

  /// LIVE: status says live/halftime/break/penalties/extra_time (no date filter)
  static List<MatchModel> _filterLiveMatches(List<MatchModel> matches) {
    return matches.where((m) => m.isLive).toList();
  }

  /// FINISHED: status says finished/ft
  static List<MatchModel> _filterFinishedMatches(List<MatchModel> matches) {
    return matches.where((m) => m.isFinished).toList();
  }

  /// SCHEDULED: not live AND not finished AND match time is strictly in the future
  static List<MatchModel> _filterScheduledMatches(List<MatchModel> matches) {
    final now = DateTime.now();
    return matches.where((m) {
      if (m.isLive || m.isFinished) return false;
      // Parse date + time to filter out past matches
      try {
        final dateParts = m.date.contains('-')
            ? m.date.split('-') // YYYY-MM-DD
            : m.date.split('.'); // DD.MM.YYYY
        final timeParts = m.time.split(':');
        if (dateParts.length >= 3 && timeParts.length >= 2) {
          final int year, month, day;
          if (m.date.contains('-')) {
            year = int.parse(dateParts[0]);
            month = int.parse(dateParts[1]);
            day = int.parse(dateParts[2]);
          } else {
            day = int.parse(dateParts[0]);
            month = int.parse(dateParts[1]);
            year = int.parse(dateParts[2]);
          }
          final matchDt = DateTime(
            year, month, day,
            int.parse(timeParts[0]),
            int.parse(timeParts[1]),
          );
          return matchDt.isAfter(now);
        }
      } catch (_) {}
      // If we can't parse the date/time, keep the match (don't hide it)
      return true;
    }).toList();
  }
}

class MatchGroupHeader {
  final String title;
  MatchGroupHeader({required this.title});
}

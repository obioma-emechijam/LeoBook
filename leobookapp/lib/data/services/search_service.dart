// search_service.dart: Fuzzy search across teams, leagues, and matches via Supabase.
// Part of LeoBook App — Services
//
// Classes: SearchService

import 'package:flutter/foundation.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class SearchService {
  final _supabase = Supabase.instance.client;

  /// Search teams and leagues using name matching + search_terms + abbreviations.
  /// Returns a list of maps with keys: id, name, type, crest, region.
  Future<List<Map<String, dynamic>>> fuzzySearch(String query) async {
    final q = query.toLowerCase().trim();
    if (q.isEmpty) return [];

    try {
      final results = <Map<String, dynamic>>[];

      // 1. Search Teams — name, search_terms, abbreviations (all text ILIKE)
      final teamResults = await _supabase
          .from('teams')
          .select('team_id, name, crest, search_terms, abbreviations')
          .or('name.ilike.%$q%,search_terms.ilike.%$q%,abbreviations.ilike.%$q%')
          .limit(10);

      for (var t in (teamResults as List)) {
        results.add({
          'id': t['team_id']?.toString() ?? '',
          'name': t['name']?.toString() ?? '',
          'type': 'team',
          'crest': t['crest']?.toString() ?? '',
        });
      }

      // 2. Search Leagues
      final leagueResults = await _supabase
          .from('leagues')
          .select(
              'league_id, continent, name, crest, search_terms, abbreviations')
          .or('name.ilike.%$q%,continent.ilike.%$q%,search_terms.ilike.%$q%,abbreviations.ilike.%$q%')
          .limit(10);

      for (var l in (leagueResults as List)) {
        final continent = l['continent']?.toString() ?? '';
        final league = l['name']?.toString() ?? '';
        final displayName =
            continent.isNotEmpty ? '$continent: $league' : league;
        results.add({
          'id': l['league_id']?.toString() ?? '',
          'name': displayName,
          'type': 'league',
          'crest': l['crest']?.toString() ?? '',
          'region': continent,
        });
      }

      return results;
    } catch (e) {
      debugPrint('[SearchService] Error: $e');
      return [];
    }
  }
}

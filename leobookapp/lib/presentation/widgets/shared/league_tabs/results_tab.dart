// results_tab.dart: Shows completed matches grouped by round for a league.
// Part of LeoBook App — League Tab Widgets

import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:cached_network_image/cached_network_image.dart';
import 'package:leobookapp/data/models/match_model.dart';
import 'package:leobookapp/data/repositories/data_repository.dart';
import 'package:leobookapp/core/constants/app_colors.dart';
import 'package:leobookapp/core/widgets/leo_shimmer.dart';
import '../match_card.dart';

class LeagueResultsTab extends StatefulWidget {
  final String leagueId;
  final String leagueName;
  final String? season;
  final VoidCallback? onStandingsTap;

  const LeagueResultsTab({
    super.key,
    required this.leagueId,
    required this.leagueName,
    this.season,
    this.onStandingsTap,
  });

  @override
  State<LeagueResultsTab> createState() => _LeagueResultsTabState();
}

class _LeagueResultsTabState extends State<LeagueResultsTab> {
  late Future<List<MatchModel>> _resultsFuture;

  @override
  void initState() {
    super.initState();
    _resultsFuture = _loadResults();
  }

  Future<List<MatchModel>> _loadResults() async {
    final repo = context.read<DataRepository>();
    final allMatches = await repo.fetchFixturesByLeague(
      widget.leagueId,
      season: widget.season,
    );
    return allMatches
        .where((m) =>
            m.status == 'Finished' ||
            m.displayStatus == 'FINISHED' ||
            m.isFinished)
        .toList()
      ..sort((a, b) {
        try {
          return DateTime.parse(b.date).compareTo(DateTime.parse(a.date));
        } catch (_) {
          return 0;
        }
      });
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<List<MatchModel>>(
      future: _resultsFuture,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const MatchListSkeleton();
        }
        if (snapshot.hasError) {
          return Center(child: Text('Error: ${snapshot.error}'));
        }

        final matches = snapshot.data ?? [];

        if (matches.isEmpty) {
          return Center(
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                const Icon(Icons.scoreboard_outlined,
                    size: 48, color: AppColors.textGrey),
                const SizedBox(height: 16),
                Text(
                  "No results found",
                  style: GoogleFonts.lexend(
                    color: AppColors.textGrey,
                    fontSize: 14,
                  ),
                ),
              ],
            ),
          );
        }

        // Group results by round (leagueStage) — fall back to month
        final Map<String, List<MatchModel>> grouped = {};
        for (final m in matches) {
          String groupKey;
          if (m.leagueStage != null && m.leagueStage!.isNotEmpty) {
            groupKey = m.leagueStage!;
          } else {
            try {
              final dt = DateTime.parse(m.date);
              const months = [
                'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'
              ];
              groupKey = '${months[dt.month - 1]} ${dt.year}';
            } catch (_) {
              groupKey = 'UNKNOWN';
            }
          }
          grouped.putIfAbsent(groupKey, () => []).add(m);
        }
        final sections = grouped.entries.toList();

        // Extract league info from first match
        final firstMatch = matches.first;
        final season = firstMatch.season ?? widget.season;
        final leagueCrest = firstMatch.leagueCrestUrl;

        return ListView.builder(
          padding: const EdgeInsets.only(top: 0, bottom: 100),
          itemCount: sections.fold<int>(0, (sum, e) => sum + 1 + e.value.length) + 1,
          itemBuilder: (context, index) {
            // First item: league info header
            if (index == 0) {
              return _ResultsLeagueHeader(
                leagueName: widget.leagueName,
                season: season,
                leagueCrestUrl: leagueCrest,
                latestRound: sections.isNotEmpty ? sections.first.key : null,
                totalResults: matches.length,
                onStandingsTap: widget.onStandingsTap,
              );
            }

            int adjustedIndex = index - 1;
            int cursor = 0;
            for (final section in sections) {
              if (adjustedIndex == cursor) {
                return _RoundHeader(title: section.key);
              }
              cursor++;
              if (adjustedIndex < cursor + section.value.length) {
                final match = section.value[adjustedIndex - cursor];
                return Padding(
                  padding: const EdgeInsets.only(bottom: 4),
                  child: MatchCard(match: match),
                );
              }
              cursor += section.value.length;
            }
            return const SizedBox.shrink();
          },
        );
      },
    );
  }
}

/// Round header
class _RoundHeader extends StatelessWidget {
  final String title;
  const _RoundHeader({required this.title});

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.fromLTRB(16, 20, 16, 8),
      padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 8),
      decoration: BoxDecoration(
        color: AppColors.primary.withValues(alpha: 0.08),
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: AppColors.primary.withValues(alpha: 0.15)),
      ),
      child: Row(
        children: [
          Container(
            width: 4,
            height: 16,
            decoration: BoxDecoration(
              color: AppColors.primary,
              borderRadius: BorderRadius.circular(2),
            ),
          ),
          const SizedBox(width: 10),
          Text(
            title.toUpperCase(),
            style: GoogleFonts.lexend(
              fontSize: 11,
              fontWeight: FontWeight.w800,
              color: AppColors.primary,
              letterSpacing: 1.5,
            ),
          ),
        ],
      ),
    );
  }
}

/// Results league header with season and result count
class _ResultsLeagueHeader extends StatelessWidget {
  final String leagueName;
  final String? season;
  final String? leagueCrestUrl;
  final String? latestRound;
  final int totalResults;
  final VoidCallback? onStandingsTap;

  const _ResultsLeagueHeader({
    required this.leagueName,
    this.season,
    this.leagueCrestUrl,
    this.latestRound,
    this.totalResults = 0,
    this.onStandingsTap,
  });

  @override
  Widget build(BuildContext context) {
    // Parse round number for progress
    int? roundNum;
    if (latestRound != null) {
      final match = RegExp(r'(\d+)').firstMatch(latestRound!);
      if (match != null) roundNum = int.tryParse(match.group(1)!);
    }

    return Container(
      margin: const EdgeInsets.fromLTRB(16, 12, 16, 4),
      padding: const EdgeInsets.all(14),
      decoration: BoxDecoration(
        color: AppColors.neutral800,
        borderRadius: BorderRadius.circular(14),
        border: Border.all(color: Colors.white10),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Row 1: Crest + name + season
          Row(
            children: [
              if (leagueCrestUrl != null && leagueCrestUrl!.isNotEmpty) ...[
                CachedNetworkImage(
                  imageUrl: leagueCrestUrl!,
                  width: 22,
                  height: 22,
                  fit: BoxFit.contain,
                  errorWidget: (_, __, ___) => const Icon(
                    Icons.emoji_events,
                    size: 18,
                    color: AppColors.primary,
                  ),
                ),
                const SizedBox(width: 8),
              ],
              Expanded(
                child: Text(
                  _parseLeagueName(leagueName),
                  style: GoogleFonts.lexend(
                    fontSize: 13,
                    fontWeight: FontWeight.w800,
                    color: Colors.white,
                  ),
                  overflow: TextOverflow.ellipsis,
                ),
              ),
              if (season != null && season!.isNotEmpty)
                Container(
                  padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
                  decoration: BoxDecoration(
                    color: AppColors.primary.withValues(alpha: 0.12),
                    borderRadius: BorderRadius.circular(6),
                    border: Border.all(color: AppColors.primary.withValues(alpha: 0.25)),
                  ),
                  child: Text(
                    season!,
                    style: GoogleFonts.lexend(
                      fontSize: 10,
                      fontWeight: FontWeight.w700,
                      color: AppColors.primary,
                    ),
                  ),
                ),
            ],
          ),

          // Row 2: Progress bar
          if (roundNum != null) ...[
            const SizedBox(height: 12),
            _buildSeasonProgress(roundNum),
          ],

          // Row 3: Result count + standings link
          const SizedBox(height: 10),
          Row(
            children: [
              Text(
                '$totalResults results',
                style: GoogleFonts.lexend(
                  fontSize: 10,
                  fontWeight: FontWeight.w600,
                  color: AppColors.textGrey,
                ),
              ),
              const Spacer(),
              if (onStandingsTap != null)
                GestureDetector(
                  onTap: onStandingsTap,
                  child: Row(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      const Icon(Icons.leaderboard, size: 14, color: AppColors.textGrey),
                      const SizedBox(width: 4),
                      Text(
                        "STANDINGS",
                        style: GoogleFonts.lexend(
                          fontSize: 10,
                          fontWeight: FontWeight.w700,
                          color: AppColors.textGrey,
                          letterSpacing: 0.5,
                        ),
                      ),
                      const Icon(Icons.chevron_right, size: 14, color: AppColors.textGrey),
                    ],
                  ),
                ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildSeasonProgress(int currentRound) {
    final totalRounds = currentRound > 34 ? 38 : 34;
    final progress = (currentRound / totalRounds).clamp(0.0, 1.0);

    return Column(
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Text(
              'Latest: $latestRound',
              style: GoogleFonts.lexend(
                fontSize: 10,
                fontWeight: FontWeight.w700,
                color: AppColors.primary,
              ),
            ),
            Text(
              '${(progress * 100).toInt()}% of season',
              style: GoogleFonts.lexend(
                fontSize: 9,
                fontWeight: FontWeight.w600,
                color: AppColors.textGrey,
              ),
            ),
          ],
        ),
        const SizedBox(height: 6),
        ClipRRect(
          borderRadius: BorderRadius.circular(4),
          child: SizedBox(
            height: 6,
            child: LinearProgressIndicator(
              value: progress,
              backgroundColor: Colors.white10,
              valueColor: AlwaysStoppedAnimation<Color>(AppColors.primary),
            ),
          ),
        ),
      ],
    );
  }

  String _parseLeagueName(String fullName) {
    if (fullName.contains(':')) {
      return fullName.split(':').last.trim();
    }
    return fullName;
  }
}

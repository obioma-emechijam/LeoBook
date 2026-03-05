// league_screen.dart: league_screen.dart: Widget/screen for App — Screens.
// Part of LeoBook App — Screens
//
// Classes: LeagueScreen, _LeagueScreenState

import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:cached_network_image/cached_network_image.dart';
import 'package:leobookapp/core/constants/app_colors.dart';
import 'package:leobookapp/data/models/league_model.dart';
import 'package:leobookapp/data/repositories/data_repository.dart';
import '../widgets/shared/leo_tab.dart';
import '../widgets/shared/main_top_bar.dart';
import '../widgets/shared/league_tabs/overview_tab.dart';
import '../widgets/shared/league_tabs/fixtures_tab.dart';
import '../widgets/shared/league_tabs/predictions_tab.dart';
import '../widgets/shared/league_tabs/stats_tab.dart';

class LeagueScreen extends StatefulWidget {
  final String leagueId;
  final String leagueName;

  const LeagueScreen({
    super.key,
    required this.leagueId,
    required this.leagueName,
  });

  @override
  State<LeagueScreen> createState() => _LeagueScreenState();
}

class _LeagueScreenState extends State<LeagueScreen>
    with SingleTickerProviderStateMixin {
  late TabController _tabController;
  final DataRepository _repo = DataRepository();
  LeagueModel? _league;

  @override
  void initState() {
    super.initState();
    _tabController = TabController(length: 4, vsync: this);
    _loadLeagueData();
  }

  Future<void> _loadLeagueData() async {
    final league = await _repo.fetchLeagueById(widget.leagueId);
    if (mounted) {
      setState(() => _league = league);
    }
  }

  @override
  void dispose() {
    _tabController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final isDark = Theme.of(context).brightness == Brightness.dark;

    return Scaffold(
      backgroundColor:
          isDark ? AppColors.backgroundDark : AppColors.backgroundLight,
      body: Column(
        children: [
          MainTopBar(
            currentIndex: -1,
            onTabChanged: (_) {},
          ),
          Expanded(
            child: NestedScrollView(
              headerSliverBuilder: (context, innerBoxIsScrolled) {
                return [
                  SliverAppBar(
                    backgroundColor: isDark
                        ? AppColors.backgroundDark.withValues(alpha: 0.9)
                        : AppColors.backgroundLight.withValues(alpha: 0.9),
                    surfaceTintColor: Colors.transparent,
                    pinned: true,
                    floating: true,
                    snap: true,
                    elevation: 0,
                    toolbarHeight: 64,
                    leading: IconButton(
                      icon: Icon(
                        Icons.arrow_back_ios_new,
                        size: 20,
                        color: isDark ? Colors.white : AppColors.textDark,
                      ),
                      onPressed: () => Navigator.pop(context),
                    ),
                    titleSpacing: 0,
                    title: Row(
                      children: [
                        Container(
                          width: 32,
                          height: 32,
                          margin: const EdgeInsets.only(right: 10),
                          decoration: BoxDecoration(
                            color: isDark ? AppColors.cardDark : Colors.white,
                            shape: BoxShape.circle,
                            border: Border.all(
                              color: isDark
                                  ? Colors.white.withValues(alpha: 0.1)
                                  : Colors.black.withValues(alpha: 0.1),
                            ),
                          ),
                          child: _league?.crest != null &&
                                  _league!.crest!.startsWith('http')
                              ? ClipOval(
                                  child: CachedNetworkImage(
                                    imageUrl: _league!.crest!,
                                    width: 32,
                                    height: 32,
                                    fit: BoxFit.cover,
                                    placeholder: (_, __) => const Icon(
                                      Icons.emoji_events_outlined,
                                      size: 18,
                                      color: AppColors.primary,
                                    ),
                                    errorWidget: (_, __, ___) => const Icon(
                                      Icons.emoji_events_outlined,
                                      size: 18,
                                      color: AppColors.primary,
                                    ),
                                  ),
                                )
                              : const Icon(
                                  Icons.emoji_events_outlined,
                                  size: 18,
                                  color: AppColors.primary,
                                ),
                        ),
                        Column(
                          mainAxisSize: MainAxisSize.min,
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(
                              widget.leagueName,
                              style: GoogleFonts.lexend(
                                fontSize: 16,
                                fontWeight: FontWeight.w700,
                                color:
                                    isDark ? Colors.white : AppColors.textDark,
                                height: 1.0,
                              ),
                            ),
                            Text(
                              (_league?.currentSeason ?? '').toUpperCase(),
                              style: GoogleFonts.lexend(
                                fontSize: 10,
                                fontWeight: FontWeight.w700,
                                color: AppColors.textGrey,
                                letterSpacing: 1.0,
                              ),
                            ),
                          ],
                        ),
                      ],
                    ),
                    actions: [
                      IconButton(
                        icon: const Icon(
                          Icons.star_border,
                          color: AppColors.textGrey,
                        ),
                        onPressed: () {},
                      ),
                      IconButton(
                        icon:
                            const Icon(Icons.search, color: AppColors.textGrey),
                        onPressed: () {},
                      ),
                    ],
                    bottom: PreferredSize(
                      preferredSize: const Size.fromHeight(50),
                      child: Container(
                        decoration: BoxDecoration(
                          border: Border(
                            bottom: BorderSide(
                              color: isDark
                                  ? Colors.white.withValues(alpha: 0.1)
                                  : Colors.black.withValues(alpha: 0.05),
                            ),
                          ),
                        ),
                        child: AnimatedBuilder(
                          animation: _tabController,
                          builder: (context, _) {
                            return TabBar(
                              controller: _tabController,
                              isScrollable: true,
                              tabAlignment: TabAlignment.start,
                              padding:
                                  const EdgeInsets.symmetric(horizontal: 16),
                              labelColor: AppColors.primary,
                              unselectedLabelColor: AppColors.textGrey,
                              indicatorColor: AppColors.primary,
                              indicatorSize: TabBarIndicatorSize.label,
                              indicatorWeight: 3,
                              dividerColor: Colors.transparent,
                              tabs: [
                                Tab(
                                  child: LeoTab(
                                    text: "OVERVIEW",
                                    isSelected: _tabController.index == 0,
                                  ),
                                ),
                                Tab(
                                  child: LeoTab(
                                    text: "FIXTURES",
                                    isSelected: _tabController.index == 1,
                                  ),
                                ),
                                Tab(
                                  child: LeoTab(
                                    text: "PREDICTIONS",
                                    isSelected: _tabController.index == 2,
                                  ),
                                ),
                                Tab(
                                  child: LeoTab(
                                    text: "STATS",
                                    isSelected: _tabController.index == 3,
                                  ),
                                ),
                              ],
                            );
                          },
                        ),
                      ),
                    ),
                  ),
                ];
              },
              body: TabBarView(
                controller: _tabController,
                children: [
                  LeagueOverviewTab(leagueName: widget.leagueName),
                  LeagueFixturesTab(leagueName: widget.leagueName),
                  LeaguePredictionsTab(leagueName: widget.leagueName),
                  LeagueStatsTab(leagueName: widget.leagueName),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }
}

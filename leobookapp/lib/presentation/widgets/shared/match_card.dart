// match_card.dart — LeoBook v9.4 Match Card (Type3/Type4 Design)
// Part of LeoBook App — Widgets
//
// Layout aligned with UI Inspiration Type3.png (upcoming) and Type4.png (finished).
// Uses LeoTypography (DM Sans), AppColors (UI Inspiration Night), SpacingScale.

import 'dart:async';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_svg/flutter_svg.dart';
import 'package:cached_network_image/cached_network_image.dart';
import 'package:leobookapp/core/constants/app_colors.dart';
import 'package:leobookapp/core/constants/responsive_constants.dart';
import 'package:leobookapp/core/constants/spacing_constants.dart';
import 'package:leobookapp/core/theme/leo_typography.dart';
import 'package:leobookapp/data/models/match_model.dart';
import '../../screens/match_details_screen.dart';

class MatchCard extends StatefulWidget {
  final MatchModel match;
  final bool showLiveBadge;
  final bool showLeagueHeader;
  final bool hideLeagueInfo;
  const MatchCard({
    super.key,
    required this.match,
    this.showLiveBadge = true,
    this.showLeagueHeader = true,
    this.hideLeagueInfo = false,
  });

  @override
  State<MatchCard> createState() => _MatchCardState();
}

class _MatchCardState extends State<MatchCard> {
  bool _isHovered = false;
  Timer? _countdownTimer;
  Duration? _timeToKickoff;

  MatchModel get match => widget.match;

  @override
  void initState() {
    super.initState();
    _startCountdown();
  }

  @override
  void dispose() {
    _countdownTimer?.cancel();
    super.dispose();
  }

  void _startCountdown() {
    final kickoff = _parseKickoffTime();
    if (kickoff == null) return;
    _updateCountdown(kickoff);
    _countdownTimer = Timer.periodic(const Duration(seconds: 1), (_) {
      _updateCountdown(kickoff);
    });
  }

  void _updateCountdown(DateTime kickoff) {
    final now = DateTime.now();
    final diff = kickoff.difference(now);
    if (diff.isNegative || diff.inHours >= 1) {
      if (_timeToKickoff != null) setState(() => _timeToKickoff = null);
      return;
    }
    setState(() => _timeToKickoff = diff);
  }

  DateTime? _parseKickoffTime() {
    try {
      final t = match.time.length == 5 ? match.time : '00:00';
      return DateTime.parse("${match.date}T$t:00");
    } catch (_) {
      return null;
    }
  }

  // Parse "Country: League Name" into parts
  (String country, String leagueName) get _leagueParts {
    String country = '';
    String leagueName = match.league ?? '';
    if (leagueName.contains(':')) {
      final parts = leagueName.split(':');
      if (parts.length >= 2) {
        country = parts[0].trim();
        leagueName = parts[1].trim();
      }
    }
    return (country, leagueName);
  }

  bool get _isFinished =>
      match.status.toLowerCase().contains('finished') ||
      match.status.toUpperCase() == 'FT' ||
      match.isFinished;

  @override
  Widget build(BuildContext context) {
    final (:country, :leagueName) = (country: _leagueParts.$1, leagueName: _leagueParts.$2);

    return Padding(
      padding: EdgeInsets.symmetric(
        horizontal: Responsive.sp(context, 4),
        vertical: Responsive.sp(context, 4),
      ),
      child: MouseRegion(
        onEnter: (_) => setState(() => _isHovered = true),
        onExit: (_) => setState(() => _isHovered = false),
        child: GestureDetector(
          onTap: () {
            HapticFeedback.lightImpact();
            Navigator.push(
              context,
              MaterialPageRoute(
                builder: (_) => MatchDetailsScreen(match: match),
              ),
            );
          },
          child: AnimatedContainer(
            duration: const Duration(milliseconds: 200),
            curve: Curves.easeOutCubic,
            transform: Matrix4.identity()..scale(_isHovered ? 1.012 : 1.0),
            transformAlignment: Alignment.center,
            decoration: BoxDecoration(
              color: AppColors.neutral800,
              borderRadius: BorderRadius.circular(SpacingScale.cardRadius),
              border: Border.all(
                color: _isHovered
                    ? AppColors.primary.withValues(alpha: 0.4)
                    : (match.isLive
                        ? AppColors.liveRed.withValues(alpha: 0.3)
                        : AppColors.neutral700),
                width: 0.5,
              ),
              boxShadow: _isHovered
                  ? [
                      BoxShadow(
                        color: AppColors.primary.withValues(alpha: 0.08),
                        blurRadius: 16,
                        spreadRadius: 1,
                      )
                    ]
                  : null,
            ),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                // ═══════════════════════════════════════════════
                // HEADER: League info + badges
                // ═══════════════════════════════════════════════
                if (widget.showLeagueHeader && !widget.hideLeagueInfo)
                  _buildLeagueHeader(context, country, leagueName),

                // ═══════════════════════════════════════════════
                // TEAMS ROW: Name [Crest] Score/VS [Crest] Name
                // ═══════════════════════════════════════════════
                Padding(
                  padding: EdgeInsets.symmetric(
                    horizontal: SpacingScale.cardPadding,
                    vertical: Responsive.sp(context, 8),
                  ),
                  child: _buildTeamsRow(context),
                ),

                // ═══════════════════════════════════════════════
                // STATUS / COUNTDOWN / DATE
                // ═══════════════════════════════════════════════
                _buildStatusSection(context),

                SizedBox(height: Responsive.sp(context, 6)),

                // ═══════════════════════════════════════════════
                // PREDICTION BAR (bottom)
                // ═══════════════════════════════════════════════
                if (match.prediction != null && match.prediction!.isNotEmpty)
                  _buildPredictionBar(context),
              ],
            ),
          ),
        ),
      ),
    );
  }

  // ─────────────────────────────────────────────────────────────
  // LEAGUE HEADER ROW
  // ─────────────────────────────────────────────────────────────
  Widget _buildLeagueHeader(
      BuildContext context, String country, String leagueName) {
    return Padding(
      padding: EdgeInsets.fromLTRB(
        SpacingScale.cardPadding,
        SpacingScale.md,
        SpacingScale.cardPadding,
        SpacingScale.xs,
      ),
      child: Row(
        children: [
          // League crest
          if (match.leagueCrestUrl != null && match.leagueCrestUrl!.isNotEmpty)
            Padding(
              padding: const EdgeInsets.only(right: SpacingScale.sm),
              child: CachedNetworkImage(
                imageUrl: match.leagueCrestUrl!,
                width: Responsive.sp(context, 14),
                height: Responsive.sp(context, 14),
                fit: BoxFit.contain,
                errorWidget: (_, __, ___) => const SizedBox.shrink(),
              ),
            ),
          // League name
          Expanded(
            child: Text(
              country.isNotEmpty ? '$country: $leagueName' : leagueName,
              style: LeoTypography.labelSmall.copyWith(
                color: AppColors.textTertiary,
                letterSpacing: 0.3,
              ),
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
            ),
          ),
          // Live badge or Accurate badge
          if (widget.showLiveBadge && (match.isLive || match.isStartingSoon))
            _LiveIndicator(
              label: match.isStartingSoon && !match.isLive
                  ? 'SOON'
                  : (match.liveMinute != null && match.liveMinute!.isNotEmpty
                      ? "LIVE ${match.liveMinute}'"
                      : 'LIVE'),
              isLive: match.isLive,
            )
          else if (_isFinished && match.isPredictionAccurate)
            Container(
              padding: const EdgeInsets.symmetric(
                horizontal: SpacingScale.sm,
                vertical: SpacingScale.xs,
              ),
              decoration: BoxDecoration(
                color: AppColors.success.withValues(alpha: 0.15),
                borderRadius: BorderRadius.circular(SpacingScale.chipRadius),
              ),
              child: Text(
                '✓ ACCURATE',
                style: LeoTypography.labelSmall.copyWith(
                  color: AppColors.success,
                  fontWeight: FontWeight.w700,
                ),
              ),
            ),
        ],
      ),
    );
  }

  // ─────────────────────────────────────────────────────────────
  // TEAMS ROW: TeamName [Crest] VS/Score [Crest] TeamName
  // ─────────────────────────────────────────────────────────────
  Widget _buildTeamsRow(BuildContext context) {
    final crestSize = Responsive.sp(context, 30);
    final isHome = _isFinished && match.winner == 'home';
    final isAway = _isFinished && match.winner == 'away';

    return Row(
      children: [
        // Home team name (left-aligned)
        Expanded(
          child: _TeamName(
            name: match.homeTeam,
            align: TextAlign.right,
            isWinner: isHome,
            redCards: match.homeRedCards,
            alignRedCardsLeft: false,
          ),
        ),
        SizedBox(width: Responsive.sp(context, 6)),
        // Home crest
        _TeamCrest(
          crestUrl: match.homeCrestUrl,
          teamName: match.homeTeam,
          size: crestSize,
        ),
        // Center: Score or VS
        Padding(
          padding: EdgeInsets.symmetric(horizontal: Responsive.sp(context, 8)),
          child: _buildCenterElement(context),
        ),
        // Away crest
        _TeamCrest(
          crestUrl: match.awayCrestUrl,
          teamName: match.awayTeam,
          size: crestSize,
        ),
        SizedBox(width: Responsive.sp(context, 6)),
        // Away team name (right-aligned)
        Expanded(
          child: _TeamName(
            name: match.awayTeam,
            align: TextAlign.left,
            isWinner: isAway,
            redCards: match.awayRedCards,
            alignRedCardsLeft: true,
          ),
        ),
      ],
    );
  }

  // Center element: Score for finished/live, VS for upcoming
  Widget _buildCenterElement(BuildContext context) {
    final hasScore = match.homeScore != null && match.awayScore != null;

    if (_isFinished && hasScore) {
      // Type4: score with winner highlighting
      final homeWins = match.winner == 'home';
      final awayWins = match.winner == 'away';
      final winColor = AppColors.success;
      final loseColor = AppColors.textTertiary.withValues(alpha: 0.6);
      final neutralColor = AppColors.textPrimary;

      return Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Text(
            '${match.homeScore}',
            style: LeoTypography.headlineMedium.copyWith(
              color: homeWins ? winColor : (awayWins ? loseColor : neutralColor),
              fontWeight: FontWeight.w900,
            ),
          ),
          Padding(
            padding: EdgeInsets.symmetric(horizontal: Responsive.sp(context, 4)),
            child: Text(
              ':',
              style: LeoTypography.headlineMedium.copyWith(
                color: AppColors.textTertiary,
                fontWeight: FontWeight.w900,
              ),
            ),
          ),
          Text(
            '${match.awayScore}',
            style: LeoTypography.headlineMedium.copyWith(
              color: awayWins ? winColor : (homeWins ? loseColor : neutralColor),
              fontWeight: FontWeight.w900,
            ),
          ),
        ],
      );
    }

    if (match.isLive && hasScore) {
      // Live score with red accent
      return Text(
        '${match.homeScore} : ${match.awayScore}',
        style: LeoTypography.headlineMedium.copyWith(
          color: AppColors.liveRed,
          fontWeight: FontWeight.w900,
        ),
      );
    }

    // Type3: "VS" for upcoming
    return Text(
      'VS',
      style: LeoTypography.titleLarge.copyWith(
        color: AppColors.textTertiary,
        fontStyle: FontStyle.italic,
        fontWeight: FontWeight.w900,
      ),
    );
  }

  // ─────────────────────────────────────────────────────────────
  // STATUS SECTION: FINISHED / Countdown / Date
  // ─────────────────────────────────────────────────────────────
  Widget _buildStatusSection(BuildContext context) {
    return Column(
      children: [
        // Status text (FINISHED, LIVE minute, etc.)
        if (match.displayStatus.isNotEmpty)
          Text(
            match.displayStatus,
            style: LeoTypography.labelSmall.copyWith(
              color: match.isLive ? AppColors.liveRed : AppColors.textTertiary,
              fontWeight: FontWeight.w700,
              letterSpacing: 1.0,
            ),
          ),

        // Countdown (only within 1hr before kickoff)
        if (_timeToKickoff != null) ...[
          SizedBox(height: Responsive.sp(context, 4)),
          _buildCountdown(context),
        ],

        SizedBox(height: Responsive.sp(context, 2)),

        // Date & Time
        Text(
          _formatDisplayDate(),
          style: LeoTypography.bodySmall.copyWith(
            color: AppColors.textTertiary,
          ),
        ),
      ],
    );
  }

  Widget _buildCountdown(BuildContext context) {
    final d = _timeToKickoff!;
    final mm = d.inMinutes.remainder(60).toString().padLeft(2, '0');
    final ss = d.inSeconds.remainder(60).toString().padLeft(2, '0');

    return Row(
      mainAxisAlignment: MainAxisAlignment.center,
      mainAxisSize: MainAxisSize.min,
      children: [
        _countdownDigit(context, '00'),
        _countdownSep(context),
        _countdownDigit(context, mm),
        _countdownSep(context),
        _countdownDigit(context, ss),
      ],
    );
  }

  Widget _countdownDigit(BuildContext context, String val) {
    return Text(
      val,
      style: LeoTypography.titleLarge.copyWith(
        color: AppColors.primary,
        fontWeight: FontWeight.w700,
        letterSpacing: 2,
      ),
    );
  }

  Widget _countdownSep(BuildContext context) {
    return Padding(
      padding: EdgeInsets.symmetric(horizontal: Responsive.sp(context, 4)),
      child: Text(
        ':',
        style: LeoTypography.titleLarge.copyWith(
          color: AppColors.textTertiary,
          fontWeight: FontWeight.w700,
        ),
      ),
    );
  }

  String _formatDisplayDate() {
    try {
      final dt = DateTime.parse(match.date);
      const months = [
        '', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
      ];
      return '${dt.day} ${months[dt.month]} ${dt.year} (${match.time})';
    } catch (_) {
      return '${match.date} (${match.time})';
    }
  }

  // ─────────────────────────────────────────────────────────────
  // PREDICTION BAR (bottom section)
  // ─────────────────────────────────────────────────────────────
  Widget _buildPredictionBar(BuildContext context) {
    final reliabilityVal =
        double.tryParse(match.marketReliability ?? '') ?? 0;
    final oddsVal = double.tryParse(match.odds ?? '') ?? 0;

    return Container(
      padding: EdgeInsets.symmetric(
        horizontal: SpacingScale.cardPadding,
        vertical: Responsive.sp(context, 8),
      ),
      decoration: BoxDecoration(
        color: match.isLive
            ? AppColors.liveRed.withValues(alpha: 0.08)
            : AppColors.neutral700.withValues(alpha: 0.5),
        borderRadius: BorderRadius.vertical(
          bottom: Radius.circular(SpacingScale.cardRadius),
        ),
      ),
      child: Row(
        children: [
          // Prediction text
          Expanded(
            child: Text(
              match.prediction ?? '',
              style: LeoTypography.labelLarge.copyWith(
                color: _isFinished
                    ? (match.isPredictionAccurate
                        ? AppColors.success
                        : AppColors.error)
                    : AppColors.textPrimary,
                decoration: _isFinished && !match.isPredictionAccurate
                    ? TextDecoration.lineThrough
                    : null,
              ),
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
            ),
          ),
          // Reliability badge
          if (reliabilityVal > 0)
            Container(
              padding: const EdgeInsets.symmetric(
                horizontal: SpacingScale.sm,
                vertical: SpacingScale.xs,
              ),
              decoration: BoxDecoration(
                color: AppColors.success.withValues(alpha: 0.15),
                borderRadius: BorderRadius.circular(SpacingScale.chipRadius),
              ),
              child: Text(
                '${reliabilityVal.toStringAsFixed(0)}%',
                style: LeoTypography.labelSmall.copyWith(
                  color: AppColors.success,
                  fontWeight: FontWeight.w700,
                ),
              ),
            ),
          // Odds + football.com logo
          if (oddsVal > 0) ...[
            SizedBox(width: Responsive.sp(context, 8)),
            Container(
              padding: EdgeInsets.symmetric(
                horizontal: Responsive.sp(context, 8),
                vertical: Responsive.sp(context, 3),
              ),
              decoration: BoxDecoration(
                color: AppColors.primary.withValues(alpha: 0.12),
                borderRadius: BorderRadius.circular(SpacingScale.chipRadius),
                border: Border.all(
                  color: AppColors.primary.withValues(alpha: 0.25),
                  width: 0.5,
                ),
              ),
              child: Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  SvgPicture.asset(
                    'assets/icons/footballcom_logo.svg',
                    width: Responsive.sp(context, 12),
                    height: Responsive.sp(context, 12),
                    colorFilter: const ColorFilter.mode(
                      AppColors.textPrimary,
                      BlendMode.srcIn,
                    ),
                  ),
                  SizedBox(width: Responsive.sp(context, 4)),
                  Text(
                    oddsVal.toStringAsFixed(2),
                    style: LeoTypography.labelLarge.copyWith(
                      color: AppColors.primary,
                    ),
                  ),
                ],
              ),
            ),
          ],
        ],
      ),
    );
  }

}

// ═══════════════════════════════════════════════════════════════
// TEAM CREST CIRCLE
// ═══════════════════════════════════════════════════════════════
class _TeamCrest extends StatelessWidget {
  final String? crestUrl;
  final String teamName;
  final double size;

  const _TeamCrest({
    required this.crestUrl,
    required this.teamName,
    required this.size,
  });

  @override
  Widget build(BuildContext context) {
    final initial = teamName.isNotEmpty ? teamName[0].toUpperCase() : '?';
    final hasCrest = crestUrl != null && crestUrl!.isNotEmpty;

    return Container(
      width: size,
      height: size,
      decoration: BoxDecoration(
        color: hasCrest ? null : AppColors.neutral700,
        shape: BoxShape.circle,
        border: hasCrest
            ? null
            : Border.all(color: AppColors.neutral600, width: 0.5),
      ),
      child: ClipOval(
        child: hasCrest
            ? CachedNetworkImage(
                imageUrl: crestUrl!,
                fit: BoxFit.contain,
                placeholder: (_, __) => Center(
                  child: Text(
                    initial,
                    style: LeoTypography.labelSmall
                        .copyWith(color: AppColors.textTertiary),
                  ),
                ),
                errorWidget: (_, __, ___) => Center(
                  child: Text(
                    initial,
                    style: LeoTypography.labelSmall
                        .copyWith(color: AppColors.textTertiary),
                  ),
                ),
              )
            : Center(
                child: Text(
                  initial,
                  style: LeoTypography.titleLarge.copyWith(
                    color: AppColors.textTertiary.withValues(alpha: 0.5),
                    fontWeight: FontWeight.w900,
                  ),
                ),
              ),
      ),
    );
  }
}

// ═══════════════════════════════════════════════════════════════
// TEAM NAME LABEL
// ═══════════════════════════════════════════════════════════════
class _TeamName extends StatelessWidget {
  final String name;
  final TextAlign align;
  final bool isWinner;
  final int redCards;
  final bool alignRedCardsLeft;

  const _TeamName({
    required this.name,
    required this.align,
    this.isWinner = false,
    this.redCards = 0,
    this.alignRedCardsLeft = false,
  });

  @override
  Widget build(BuildContext context) {
    final nameWidget = Text(
      name,
      textAlign: align,
      maxLines: 2,
      overflow: TextOverflow.ellipsis,
      style: LeoTypography.bodyMedium.copyWith(
        color: isWinner ? AppColors.success : AppColors.textPrimary,
        fontWeight: isWinner ? FontWeight.w800 : FontWeight.w600,
      ),
    );

    if (redCards <= 0) return nameWidget;

    // Red card badge
    final redCardBadge = Container(
      padding: const EdgeInsets.symmetric(horizontal: 4, vertical: 1),
      decoration: BoxDecoration(
        color: const Color(0xFFDC0000).withValues(alpha: 0.15),
        borderRadius: BorderRadius.circular(4),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Container(
            width: 8,
            height: 10,
            decoration: BoxDecoration(
              color: const Color(0xFFDC0000),
              borderRadius: BorderRadius.circular(1.5),
            ),
          ),
          if (redCards > 1) ...[
            const SizedBox(width: 2),
            Text(
              '×$redCards',
              style: LeoTypography.labelSmall.copyWith(
                color: const Color(0xFFDC0000),
                fontWeight: FontWeight.w700,
                fontSize: 9,
              ),
            ),
          ],
        ],
      ),
    );

    return Column(
      crossAxisAlignment: alignRedCardsLeft
          ? CrossAxisAlignment.start
          : CrossAxisAlignment.end,
      mainAxisSize: MainAxisSize.min,
      children: [
        nameWidget,
        const SizedBox(height: 2),
        redCardBadge,
      ],
    );
  }
}

// ═══════════════════════════════════════════════════════════════
// LIVE INDICATOR (pill badge)
// ═══════════════════════════════════════════════════════════════
class _LiveIndicator extends StatefulWidget {
  final String label;
  final bool isLive;

  const _LiveIndicator({required this.label, required this.isLive});

  @override
  State<_LiveIndicator> createState() => _LiveIndicatorState();
}

class _LiveIndicatorState extends State<_LiveIndicator>
    with SingleTickerProviderStateMixin {
  late AnimationController _controller;
  late Animation<double> _animation;

  @override
  void initState() {
    super.initState();
    _controller = AnimationController(
      duration: const Duration(seconds: 2),
      vsync: this,
    )..repeat(reverse: true);
    _animation = Tween<double>(begin: 1.0, end: 0.4).animate(_controller);
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final color = widget.isLive ? AppColors.liveRed : AppColors.warning;
    return FadeTransition(
      opacity: widget.isLive ? _animation : const AlwaysStoppedAnimation(1.0),
      child: Container(
        padding: const EdgeInsets.symmetric(
          horizontal: SpacingScale.sm,
          vertical: SpacingScale.xs,
        ),
        decoration: BoxDecoration(
          color: color.withValues(alpha: 0.15),
          borderRadius: BorderRadius.circular(SpacingScale.chipRadius),
        ),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Container(
              width: 6,
              height: 6,
              decoration: BoxDecoration(
                color: color,
                shape: BoxShape.circle,
              ),
            ),
            const SizedBox(width: SpacingScale.xs),
            Text(
              widget.label,
              style: LeoTypography.labelSmall.copyWith(
                color: color,
                fontWeight: FontWeight.w900,
              ),
            ),
          ],
        ),
      ),
    );
  }
}

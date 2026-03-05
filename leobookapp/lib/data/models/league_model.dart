// league_model.dart: Model for league data from Supabase.
// Part of LeoBook App — Data Models
//
// Classes: LeagueModel

class LeagueModel {
  final String leagueId;
  final String? fsLeagueId;
  final String name;
  final String? crest; // Supabase public URL
  final String? continent;
  final String? region;
  final String? regionFlag;
  final String? currentSeason;
  final String? countryCode;
  final String? url;

  LeagueModel({
    required this.leagueId,
    this.fsLeagueId,
    required this.name,
    this.crest,
    this.continent,
    this.region,
    this.regionFlag,
    this.currentSeason,
    this.countryCode,
    this.url,
  });

  /// The region_league display string (e.g. "Europe: Premier League")
  String get regionLeague =>
      continent != null && continent!.isNotEmpty ? '$continent: $name' : name;

  factory LeagueModel.fromJson(Map<String, dynamic> json) {
    return LeagueModel(
      leagueId: json['league_id']?.toString() ?? '',
      fsLeagueId: json['fs_league_id']?.toString(),
      name: json['name']?.toString() ?? '',
      crest: json['crest']?.toString(),
      continent: json['continent']?.toString(),
      region: json['region']?.toString(),
      regionFlag: json['region_flag']?.toString(),
      currentSeason: json['current_season']?.toString(),
      countryCode: json['country_code']?.toString(),
      url: json['url']?.toString(),
    );
  }
}

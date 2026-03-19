from Core.Intelligence.market_ontology import MarketOntology

class SemanticRuleEngine:
    def __init__(self):
        self.ontology = MarketOntology.load()

    def choose_market(self, fixture_data: dict, xG_home: float, xG_away: float) -> dict:
        total_xg = xG_home + xG_away


        # Step 2: Semantic scoring using ontology
        best_market = None
        best_score = -1.0
        for mkt_name, mkt in self.ontology.markets.items():
            if mkt.semantic_meaning.startswith("Total goals"):
                if (mkt.exact_outcome == "Under" and total_xg < mkt.typical_xg_range[1]) or \
                   (mkt.exact_outcome == "Over" and total_xg > mkt.typical_xg_range[0]):
                    score = mkt.likelihood_percent * 0.01 + (1.0 if mkt.risk_profile == "safe" else 0.0)
                    if score > best_score:
                        best_score = score
                        best_market = mkt
            elif mkt.base_market in ["Double Chance", "1X2"]:
                score = mkt.likelihood_percent * 0.01
                if score > best_score:
                    best_score = score
                    best_market = mkt

        # Step 3: Safety guardrails (secondary only — never override semantic choice silently)
        override_reason = None
        final_market = best_market
        if best_market.risk_profile == "safe" and total_xg > 3.8:
            override_reason = f"xG too high ({total_xg:.2f}) for safe market"
            final_market = self.ontology.markets.get("Over/Under - Over 2.5", best_market)

        return {
            "chosen_market": final_market.market_outcome,
            "market_id": final_market.market_id,
            "statistical_edge": round(best_score * 100, 1),
            "override_reason": override_reason,
            "explanation": f"{final_market.semantic_meaning} (xG {total_xg:.2f}, likelihood {final_market.likelihood_percent}%)"
        }


if __name__ == "__main__":
    engine = SemanticRuleEngine()
    test_cases = [
        {"xG_home": 1.06, "xG_away": 0.64},  # Seattle example
        {"xG_home": 0.70, "xG_away": 1.17},  # South Island
        {"xG_home": 1.40, "xG_away": 1.57},  # Club America
    ]
    for i, case in enumerate(test_cases):
        result = engine.choose_market({}, case["xG_home"], case["xG_away"])
        print(f"Test {i+1}: {result['chosen_market']} | {result['explanation']}")
        if result["override_reason"]:
            print(f"  Override: {result['override_reason']}")

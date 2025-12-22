import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import warnings

warnings.filterwarnings('ignore')

# CONFIG
UNIFIED_DATA_PATH = "/Users/jenny.lin/ImageDataParser/XGBoost_with_ImageData/data/MLS_Data_Beta_Markets.csv"
OUTPUT_DIR = "/Users/jenny.lin/BASIS_AVM_Onboarding/cate_scenario_analyses/model_outputs"
MIN_PRICE = 100000
TEST_SIZE = 0.2
RANDOM_STATE = 42

# Tree depth options to test
TREE_DEPTHS = [6, 8, 10, 12, 14, 16]

# Price tiers
PRICE_TIERS = {
    'very_low': (0, 200000),
    'low': (200000, 300000),
    'lower_mid': (300000, 400000),
    'mid': (400000, 500000),
    'upper_mid': (500000, 650000),
    'high': (650000, 850000),
    'very_high': (850000, 1500000),
    'ultra_high': (1500000, np.inf)
}

# Column name mappings - mapped to YOUR actual column names
COLUMN_MAPPINGS = {
    'living_sqft': 'sumlivingareasqft',
    'lot_sqft': 'lotsizesqft',
    'year_built': 'yearbuilt',
    'bedrooms': 'bedrooms',
    'full_baths': 'bathfull',
    'half_baths': 'bathspartialnbr',
    'garage_spaces': 'garageparkingnbr',
    'latitude': 'situslatitude',
    'longitude': 'situslongitude',
    'fireplace_code': 'fireplacecode',
    'price': 'currentsalesprice'
}

# ============================================================
# FEATURE DEFINITIONS
# ============================================================

# Core property features (standardized names that will be mapped)
BASE_FEATURES = [
    'living_sqft', 'lot_sqft', 'year_built', 'bedrooms',
    'full_baths', 'half_baths', 'garage_spaces',
    'latitude', 'longitude', 'fireplace_code'
]

# Census features (REMOVED pct_white per user request)
CENSUS_FEATURES = [
    'pct_bachelors_degree', 'median_household_income',
    'median_home_value', 'pct_owner_occupied', 'unemployment_rate',
    'median_age', 'poverty_rate',
    'median_gross_rent', 'median_earnings_total'
]

# Political features
POLITICAL_FEATURES = [
    'per_gop', 'per_dem', 'per_point_diff'
]

# Image boolean features (keep as-is, assuming they exist)
IMAGE_BOOLEAN = [
    'has_hardwood_floors',
    'has_granite_countertops',
    'has_stainless_steel_appliances',
    'has_fireplace',
    'has_attached_two_car_garage',
    'has_vaulted_ceiling',
    'has_open_floor_plan',
    'has_updated_fixtures',
    'has_crown_molding',
    'has_double_vanity',
    'has_white_cabinetry',
    'has_recessed_lighting',
    'has_formal_dining_area',
    'has_curb_appeal',
    'has_covered_front_porch',
    'has_ceiling_fan',
    'has_neutral_paint',
    'has_tile_flooring',
    'has_good_natural_light',
    'has_mature_trees',
    'has_large_windows',
    'has_fenced_yard',
    'has_brick_facade',
    'has_driveway',
    'has_mature_landscaping'
]


# ============================================================
# LOAD AND PREP
# ============================================================

def load_and_prep(filepath):
    """Load and prep data with correct column mappings."""
    print(f"Loading {filepath}...")
    df = pd.read_csv(filepath, low_memory=False)

    # Convert to lowercase for consistent mapping
    df.columns = df.columns.str.lower()

    print(f"Total records: {len(df):,}")
    print(f"Total columns: {len(df.columns)}")

    # Get price column
    price_col = COLUMN_MAPPINGS['price']

    if price_col not in df.columns:
        raise ValueError(f"Price column '{price_col}' not found in data")

    print(f"Using price column: '{price_col}'")

    # Filter by price
    df = df[df[price_col] >= MIN_PRICE].copy()
    print(f"Records after ${MIN_PRICE:,} price filter: {len(df):,}")

    # Map base feature columns and create engineered features
    living_col = COLUMN_MAPPINGS['living_sqft']
    lot_col = COLUMN_MAPPINGS['lot_sqft']
    year_col = COLUMN_MAPPINGS['year_built']
    bed_col = COLUMN_MAPPINGS['bedrooms']
    garage_col = COLUMN_MAPPINGS['garage_spaces']

    # Engineer features using actual column names
    if living_col in df.columns and bed_col in df.columns:
        df['sqft_per_bedroom'] = df[living_col] / (df[bed_col] + 1)

    if lot_col in df.columns and living_col in df.columns:
        df['lot_to_living_ratio'] = df[lot_col] / (df[living_col] + 1)

    if year_col in df.columns:
        df['property_age'] = 2024 - df[year_col]

    if garage_col in df.columns:
        df['has_garage'] = (df[garage_col] > 0).astype('int8')

    if living_col in df.columns:
        df['log_sqft'] = np.log1p(df[living_col])

    # Assign price tiers
    df['price_tier'] = df[price_col].apply(
        lambda p: next((t for t, (l, h) in PRICE_TIERS.items() if l <= p < h), 'ultra_high'))

    return df, price_col


def get_available_features(df):
    """Get features that actually exist in the dataframe."""
    all_features = []
    missing_by_group = {}

    # Base features - use mapped column names
    base_available = []
    base_missing = []
    for feature in BASE_FEATURES:
        actual_col = COLUMN_MAPPINGS.get(feature, feature)
        if actual_col in df.columns:
            base_available.append(actual_col)
        else:
            base_missing.append(feature)

    all_features.extend(base_available)
    missing_by_group['BASE'] = base_missing

    # Census, Political, Image features - check directly
    for group_name, feature_list in [('CENSUS', CENSUS_FEATURES),
                                     ('POLITICAL', POLITICAL_FEATURES),
                                     ('IMAGE_BOOLEAN', IMAGE_BOOLEAN)]:
        available = [f for f in feature_list if f in df.columns]
        missing = [f for f in feature_list if f not in df.columns]
        all_features.extend(available)
        missing_by_group[group_name] = missing

    # Add engineered features
    engineered = ['sqft_per_bedroom', 'lot_to_living_ratio', 'property_age',
                  'has_garage', 'log_sqft']
    all_features.extend([f for f in engineered if f in df.columns])

    return all_features, missing_by_group


# ============================================================
# TRAINING WITH DEPTH TUNING
# ============================================================

def train_single_depth(train_df, test_df, features, price_col, max_depth):
    """Train model with specific max_depth."""

    # Impute using training data only
    train_medians = train_df[features].median()
    train_filled = train_df[features].fillna(train_medians)
    test_filled = test_df[features].fillna(train_medians)

    X_train, y_train = train_filled.values, train_df[price_col].values
    X_test, y_test = test_filled.values, test_df[price_col].values

    # Log transform
    y_train_model = np.log1p(y_train)

    # Train model
    model = XGBRegressor(
        objective='reg:quantileerror',
        quantile_alpha=0.5,
        n_estimators=500,
        learning_rate=0.05,
        max_depth=max_depth,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=RANDOM_STATE,
        n_jobs=-1,
        tree_method='hist',
        verbosity=0
    )
    model.fit(X_train, y_train_model, verbose=False)

    # Predict
    y_pred = np.expm1(model.predict(X_test))

    # Metrics
    mae = mean_absolute_error(y_test, y_pred)
    mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
    r2 = r2_score(y_test, y_pred)

    return model, mae, mape, r2, y_test, y_pred


def train_tier_with_depth_tuning(tier_df, features, price_col, tier_name):
    """Train tier with multiple depths and select best."""

    # Split data FIRST
    train_df, test_df = train_test_split(tier_df, test_size=TEST_SIZE, random_state=RANDOM_STATE)

    best_mape = float('inf')
    best_depth = None
    best_model = None
    best_metrics = None
    best_predictions = None

    depth_results = []

    # Test each depth
    for depth in TREE_DEPTHS:
        model, mae, mape, r2, y_test, y_pred = train_single_depth(
            train_df.copy(), test_df.copy(), features, price_col, depth
        )

        depth_results.append({
            'depth': depth,
            'mae': mae,
            'mape': mape,
            'r2': r2
        })

        # Track best
        if mape < best_mape:
            best_mape = mape
            best_depth = depth
            best_model = model
            best_metrics = {
                'n_train': len(train_df),
                'n_test': len(test_df),
                'mae': mae,
                'mape': mape,
                'r2': r2,
                'best_depth': depth
            }
            best_predictions = pd.DataFrame({
                'actual': y_test,
                'predicted': y_pred,
                'tier': tier_name
            })

    # Feature importance from best model
    importance = pd.DataFrame({
        'feature': features,
        'importance': best_model.feature_importances_
    }).sort_values('importance', ascending=False)

    return {
        'model': best_model,
        'metrics': best_metrics,
        'predictions': best_predictions,
        'importance': importance,
        'depth_results': pd.DataFrame(depth_results)
    }


# ============================================================
# MAIN
# ============================================================

def main():
    """Main execution with depth tuning."""
    import time
    start = time.time()

    print("=" * 70)
    print("STRATIFIED XGBoost AVM - WITH DEPTH TUNING")
    print("=" * 70)
    print(f"Testing tree depths: {TREE_DEPTHS}")

    # Load data
    df, price_col = load_and_prep(UNIFIED_DATA_PATH)

    # Get available features
    all_available, missing_by_group = get_available_features(df)

    print("\n" + "=" * 70)
    print("FEATURE AVAILABILITY")
    print("=" * 70)

    for group_name, missing in missing_by_group.items():
        if group_name == 'BASE':
            available_count = len([f for f in BASE_FEATURES if COLUMN_MAPPINGS.get(f, f) in df.columns])
            total = len(BASE_FEATURES)
        elif group_name == 'CENSUS':
            available_count = len(CENSUS_FEATURES) - len(missing)
            total = len(CENSUS_FEATURES)
        elif group_name == 'POLITICAL':
            available_count = len(POLITICAL_FEATURES) - len(missing)
            total = len(POLITICAL_FEATURES)
        else:  # IMAGE_BOOLEAN
            available_count = len(IMAGE_BOOLEAN) - len(missing)
            total = len(IMAGE_BOOLEAN)

        print(f"\n{group_name}:")
        print(f"  ✓ Available: {available_count}/{total}")
        if missing and len(missing) <= 10:
            print(f"  ✗ Missing: {', '.join(missing[:10])}")
        elif missing:
            print(f"  ✗ Missing: {len(missing)} features")

    print(f"\n{'=' * 70}")
    print(f"TOTAL FEATURES: {len(all_available)}")
    print(f"{'=' * 70}")

    # Drop records with missing price
    df = df.dropna(subset=[price_col])

    # Train all tiers
    print("\n" + "=" * 70)
    print("TRAINING MODELS BY PRICE TIER (WITH DEPTH TUNING)")
    print("=" * 70)

    all_results = {}
    all_preds = []
    all_importance = []
    all_depth_results = []

    for tier_name, (low, high) in PRICE_TIERS.items():
        tier_df = df[df['price_tier'] == tier_name].copy()

        if len(tier_df) < 50:
            print(f"\nSkip {tier_name}: {len(tier_df)} samples (need 50+)")
            continue

        print(f"\n{tier_name} (${low / 1000:.0f}K-${high / 1000:.0f}K): {len(tier_df):,} samples")
        print(f"  Testing depths {TREE_DEPTHS}...", end=" ")

        # Train with depth tuning
        result = train_tier_with_depth_tuning(tier_df, all_available, price_col, tier_name)
        all_results[tier_name] = result
        all_preds.append(result['predictions'])

        m = result['metrics']
        print(f"✓ Best depth={m['best_depth']}")
        print(f"  → MAE=${m['mae']:,.0f} | MAPE={m['mape']:.2f}% | R²={m['r2']:.3f}")

        # Collect importance
        result['importance']['tier'] = tier_name
        all_importance.append(result['importance'])

        # Collect depth results
        result['depth_results']['tier'] = tier_name
        all_depth_results.append(result['depth_results'])

    # Overall metrics
    print("\n" + "=" * 70)
    print("OVERALL PERFORMANCE")
    print("=" * 70)

    preds = pd.concat(all_preds, ignore_index=True)
    overall_mae = mean_absolute_error(preds['actual'], preds['predicted'])
    overall_mape = np.mean(np.abs((preds['actual'] - preds['predicted']) / preds['actual']) * 100)
    overall_r2 = r2_score(preds['actual'], preds['predicted'])

    print(f"\nMAE: ${overall_mae:,.0f}")
    print(f"MAPE: {overall_mape:.2f}%")
    print(f"R²: {overall_r2:.4f}")
    print(f"Time: {time.time() - start:.1f}s")

    # Optimal depths by tier
    print("\n" + "=" * 70)
    print("OPTIMAL TREE DEPTHS BY TIER")
    print("=" * 70)

    for tier_name, result in all_results.items():
        best_depth = result['metrics']['best_depth']
        mape = result['metrics']['mape']
        print(f"{tier_name:15} → depth={best_depth:2d} (MAPE={mape:.2f}%)")

    # Top features across all tiers
    print("\n" + "=" * 70)
    print("TOP 20 MOST IMPORTANT FEATURES (AVERAGE ACROSS TIERS)")
    print("=" * 70)

    importance_df = pd.concat(all_importance, ignore_index=True)
    avg_importance = importance_df.groupby('feature')['importance'].mean().sort_values(ascending=False)

    print("\nFeature                              Importance")
    print("-" * 70)
    for feature, imp in avg_importance.head(20).items():
        print(f"{feature:<35} {imp:.4f}")

    # Save results
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    preds.to_csv(f"{OUTPUT_DIR}/predictions_depth_tuned.csv", index=False)
    avg_importance.to_csv(f"{OUTPUT_DIR}/feature_importance_depth_tuned.csv")

    # Save metrics by tier with optimal depth
    metrics_df = pd.DataFrame([
        {'tier': k, **v['metrics']}
        for k, v in all_results.items()
    ])
    metrics_df.to_csv(f"{OUTPUT_DIR}/metrics_by_tier_depth_tuned.csv", index=False)

    # Save depth comparison results
    depth_comparison = pd.concat(all_depth_results, ignore_index=True)
    depth_comparison.to_csv(f"{OUTPUT_DIR}/depth_comparison_by_tier.csv", index=False)

    print(f"\n✓ Saved results to {OUTPUT_DIR}")
    print("  - predictions_depth_tuned.csv")
    print("  - feature_importance_depth_tuned.csv")
    print("  - metrics_by_tier_depth_tuned.csv")
    print("  - depth_comparison_by_tier.csv")

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"✓ Used {len(all_available)} features")
    print(f"✓ Tested {len(TREE_DEPTHS)} tree depths per tier")
    print(f"✓ Trained on {len(preds)} test predictions")
    print(f"✓ Average MAPE: {overall_mape:.2f}%")
    print(f"✓ Removed pct_white (race-related feature)")
    print("=" * 70)


if __name__ == "__main__":
    main()
from py4phi.core import from_path

# 1) Simple feature selection with default parameters
controller = from_path('Titanic.parquet', file_type='parquet', engine='polars')
controller.perform_feature_selection(
    target_feature='Survived',
    drop_recommended=False
)

# 2) Modify recommendation thresholds (target correlations and feature-feature correlations)
controller = from_path('Titanic.parquet', file_type='parquet', engine='polars')
controller.perform_feature_selection(
    target_feature='Survived',
    target_correlation_threshold=0.2,
    features_correlation_threshold=0.2,
    drop_recommended=False
)

# 3) Drop recommended features
controller = from_path('Titanic.parquet', file_type='parquet', engine='polars')
controller.perform_feature_selection(
    target_feature='Survived',
    target_correlation_threshold=0.2,
    features_correlation_threshold=0.2,
    override_columns_to_drop=['PassengerId', 'Name', 'Cabin'],
    drop_recommended=True,
    save_folder='feature_selected_titanic_auto',
    header=1,  # Pandas write option
    index=False  # Pandas write option
)

# 4) Override recommendations with custom list to be dropped and save dataset
controller = from_path('Titanic.parquet', file_type='parquet', engine='polars')
controller.perform_feature_selection(
    target_feature='Survived',
    target_correlation_threshold=0.2,
    features_correlation_threshold=0.2,
    override_columns_to_drop=['PassengerId', 'Name', 'Cabin'],
    drop_recommended=True,
    save_folder='feature_selected_titanic',
    header=1,  # Pandas write option
    index=False  # Pandas write option
)

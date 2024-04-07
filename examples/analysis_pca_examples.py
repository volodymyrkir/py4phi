from py4phi.core import from_path


# PCA can only be used to analyze explained variance of numeric features,
# so all non-numeric columns should be ignored with "ignore_features".
# Also, one feature name can be passed to "target_feature" attribute.

# You can ignore columns that do not need to be reduced

# 1) Simple PCA with default parameters.
# Do not save reduced dataset, just suggest number of components
controller = from_path('Titanic.parquet', file_type='parquet', engine='pyspark')
controller.perform_pca(
    target_feature='Survived',
    ignore_columns=['Name', 'Sex', 'Ticket', 'Cabin', 'Embarked'],
    save_reduced=False
)

# 2) Enforce number of components to be saved
controller.perform_pca(
    target_feature='Survived',
    ignore_columns=['Name', 'Sex', 'Ticket', 'Cabin', 'Embarked'],
    n_components=3,
    save_reduced=False
)

# 3) Save result reduced dataframe, also set explained variance threshold for component number recommendation,
# change null handling algorithm to drop all rows with NA values.
# The default behavior is "fill" method,
# which uses mean by class to fill NA value if target feature is provided,
# otherwise fills with a mean by column.
controller.perform_pca(
    target_feature='Survived',
    ignore_columns=['Name', 'Sex', 'Ticket', 'Cabin', 'Embarked'],
    rec_threshold=0.7,
    save_reduced=True,
    nulls_mode='drop',
    save_folder='pca',
    index=False  # Pandas csv write option
)

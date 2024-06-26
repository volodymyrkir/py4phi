# ----------------------------------------------- ENCRYPTION/DECRYPTION -----------------------------------------------------------------
# encrypt and save
py4phi encrypt-and-save -i ./dataset.csv -c ACF -c 'Staff involved' -e polars -p -o ./

# encrypt and save with params
py4phi encrypt-and-save -i ./dataset.csv -c ACF -c 'Staff involved' -p -o ./ -r header True -e pyspark

# encrypt and print
py4phi encrypt -i ./dataset.csv -e pyspark -c ACF -c 'Staff involved' -r header True

#decrypt and save with params
py4phi decrypt-and-save -i ./py4phi_encrypted_outputs/output_dataset.csv -e pyspark -c ACF -c 'Staff involved' -p -o ./ -r header True

# encrypt & config not encrypted
 py4phi encrypt-and-save -i ./dataset.csv -c ACF -c 'Staff involved' -p -o ./ --disable_config_encryption -w index false

#decrypt and print with options
py4phi decrypt -i ./py4phi_encrypted_outputs/output_dataset.csv -c ACF -c 'Staff involved' -p -o ./ --config_not_encrypted

#encrypt model or folder and save it. Note that encryption is done inplace. Please save original before encryption.
py4phi encrypt-model -p ./py4phi_encrypted_outputs/

#decrypt model or folder
py4phi decrypt-model -p ./py4phi_encrypted_outputs/

# encrypt model/folder, do not encrypt config. Note that encryption is done inplace. Please save original before encryption.
py4phi encrypt-model -p ./py4phi_encrypted_outputs/ -d

# decrypt model/folder when config is not encrypted
py4phi decrypt-model -p ./py4phi_encrypted_outputs/ -c

# ---------------------------------------------------------- ANALYSIS-----------------------------------------------------------------
#perform PCA with target feature, ignoring column "ACF', using variance threshold recommendation set to 0.87, with 5 components.
#Also, saving the result dataframe to the current directory.
py4phi perform-pca -i ./dataset.csv  --target 'Staff involved' -c ACF --cum_var_threshold 0.87 --num_components 5 -o ./ -s -w index false

# Do the same and save in the PARQUET format
py4phi perform-pca -i ./dataset.csv  --target 'Staff involved' -c ACF --cum_var_threshold 0.87 --num_components 5 -o ./ -s -w index false --save_type parquet

# Perform analysis on a parquet file, set explained variance threshold to 80%
py4phi perform-pca -i ./Titanic.parquet  -t parquet --target Embarked -c Name -c Sex -c Cabin -c Ticket -c Age --cum_var_threshold 0.8

# Only perform analysis without saving the outputs
py4phi perform-pca -i ./dataset.csv  --target 'Staff involved' -c ACF

# Perform feature selection (only analyze correlations)
py4phi feature-selection -i ./Titanic.parquet --target Survived --target_corr_threshold 0.3 --feature_corr_threshold 0.55

# Perform feature selection and save outputs without a pandas index into the current directory.
py4phi feature-selection -i ./Titanic.parquet --target Survived --target_corr_threshold 0.05 --feature_corr_threshold 0.52 --save_reduced -w index false

# Ignore feature selection recommendations and drop columns Pclass, Name, PassengerId, save to the current directory.
py4phi feature-selection -i ./Titanic.parquet --target Survived -c Pclass -c Name -c PassengerId --save_reduced -w index false

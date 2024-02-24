# encrypt and save
py4phi encrypt-and-save -i ../py4phi/dataset.csv -c ACF -c 'Staff involved' -e polars -p -o ./

# encrypt and save with params
py4phi encrypt-and-save -i ../py4phi/dataset.csv -c ACF -c 'Staff involved' -p -o ./ -r header True

# encrypt and print
py4phi encrypt -i ../py4phi/dataset.csv -c ACF -c 'Staff involved' -r header True

#decrypt and save with params
py4phi decrypt-and-save -i ./py4phi_encrypted_outputs/output_dataset.csv -c ACF -c 'Staff involved' -p -o ./ -r header True

#decrypt and print with options
py4phi decrypt -i ./py4phi_encrypted_outputs/output_dataset.csv -c ACF -c 'Staff involved' -p -o ./ -r header True --config_not_encrypted

# config not encrypted
py4phi encrypt-and-save -i ../py4phi/dataset.csv -c ACF -c 'Staff involved' -p -o ./ -r header True --disable_config_encryption

# decrypt and save when config is not encrypted
py4phi decrypt-and-save -i ./py4phi_encrypted_outputs/output_dataset.csv -c ACF -c 'Staff involved' -p -o ./ -r header True --config_not_encrypted


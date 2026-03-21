create table if not exists raw.ruc_lsoa_2021 (
  lsoa21cd text,
  lsoa21nm text,
  lsoa21nmw text,
  ruc21cd text,
  ruc21nm text,
  urban_rural_flag text,
  objectid integer
);

TRUNCATE TABLE raw.ruc_lsoa_2021;
COPY raw.ruc_lsoa_2021 FROM '/LSOA/Rural_Urban_Classification_(2021)_of_LSOAs_in_EW.csv' CSV HEADER;

create table if not exists raw.onspd (
  pcd7 text, pcd8 text, pcds text, dointr text, doterm text,
  cty25cd text, ced25cd text, lad25cd text, wd25cd text, parncp25cd text,
  usrtypind text, east1m text, north1m text, gridind text, hlth19cd text,
  nhser24cd text, ctry25cd text, rgn25cd text, ssr95cd text, pcon24cd text,
  eer20cd text, educ23cd text, ttwa15cd text, pco19cd text, itl25cd text,
  wdstl05cd text, oa01cd text, wdcas03cd text, npark16cd text, lsoa01cd text,
  msoa01cd text, ruc01ind text, oac01ind text, oa11cd text, lsoa11cd text,
  msoa11cd text, wz11cd text, sicbl24cd text, bua24cd text, ruc11ind text,
  oac11ind text, lat double precision, long double precision, lep21cd1 text,
  lep21cd2 text, pfa23cd text, imd20ind text, cal24cd text, icb23cd text,
  oa21cd text, lsoa21cd text, msoa21cd text, ruc21ind text
);

TRUNCATE TABLE raw.onspd;
COPY raw.onspd FROM '/ONSPD/ONSPD_FEB_2026_UK.csv' CSV HEADER;

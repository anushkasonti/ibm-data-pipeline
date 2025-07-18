SELECT * FROM (cos://us-south/grp-transform-anushka/DIM_ACTIVE_APP STORED AS CSV)
UNION ALL
SELECT * FROM (cos://us-south/grp-transform-anushka/DIM_ACTIVE_UPD  STORED AS CSV)

INTO cos://us-south/grp-transform-anushka/DIM_ACTIVE JOBPREFIX NONE STORED AS CSV
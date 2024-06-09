-- Important to think of the order of the conditions --> priorities

WITH categories AS(
SELECT *

,CASE --Start Category 1
WHEN REGEXP_CONTAINS(Search_Term, r'männer') 
OR REGEXP_CONTAINS(Search_Term, r'mann')
OR REGEXP_CONTAINS(Search_Term, r'männ') 
THEN "Men's Health"
ELSE "Women's Health"
END AS Category_1, --End Category 1

CASE --Start Category 2

WHEN REGEXP_CONTAINS(Search_Interest, r'Adjuvante Chemotherapie') 
OR REGEXP_CONTAINS(Search_Interest, r'Mammakarzinom') 
OR REGEXP_CONTAINS(Search_Interest, r'Chemotherapie') 
OR REGEXP_CONTAINS(Search_Interest, r'Bestrahlung') 
OR REGEXP_CONTAINS(Search_Interest, r'Body Mind therapie') 
OR REGEXP_CONTAINS(Search_Interest, r'Hormontherapie') 
OR REGEXP_CONTAINS(Search_Interest, r'Meditation') 
OR REGEXP_CONTAINS(Search_Interest, r'Brustkrebs')
OR REGEXP_CONTAINS(Search_Term, r'karzinom') 
OR REGEXP_CONTAINS(Search_Term, r'brust') 
OR REGEXP_CONTAINS(Search_Term, r'chemo') 
OR REGEXP_CONTAINS(Search_Term, r'brest cancer') 
OR REGEXP_CONTAINS(Search_Term, r'strahlung') 
OR REGEXP_CONTAINS(Search_Term, r'körpertherapie') 
OR REGEXP_CONTAINS(Search_Term, r'mind body') 
OR REGEXP_CONTAINS(Search_Term, r'krebs') 
THEN "Cancer"

ELSE "Non-Cancer" 

END AS Category_2 --End Category 2

,CASE --Start Category 3

WHEN REGEXP_CONTAINS(Search_Interest, r'Adjuvante Chemotherapie') 
OR REGEXP_CONTAINS(Search_Interest, r'Mammakarzinom') 
OR REGEXP_CONTAINS(Search_Interest, r'Chemotherapie') 
OR REGEXP_CONTAINS(Search_Interest, r'Bestrahlung') 
OR REGEXP_CONTAINS(Search_Interest, r'Body Mind therapie') 
OR REGEXP_CONTAINS(Search_Interest, r'Brustkrebs') 
OR REGEXP_CONTAINS(Search_Term, r'karzinom') 
OR REGEXP_CONTAINS(Search_Term, r'brust') 
OR REGEXP_CONTAINS(Search_Term, r'chemo') 
OR REGEXP_CONTAINS(Search_Term, r'brest cancer') 
OR REGEXP_CONTAINS(Search_Term, r'strahlung') 
OR REGEXP_CONTAINS(Search_Term, r'strahlen') 
OR REGEXP_CONTAINS(Search_Term, r'körpertherapie') 
OR REGEXP_CONTAINS(Search_Term, r'mind body') 
THEN "Breast Cancer"

WHEN REGEXP_CONTAINS(Search_Term, r'krebs')
AND NOT REGEXP_CONTAINS(Search_Term, r'brust')
AND Search_Term NOT LIKE '% krebs %' AND Search_Term NOT LIKE '% krebs%'AND Search_Term NOT LIKE '%krebs %'
THEN 'Other Cancers'

WHEN Search_Term LIKE
'% krebs %' OR Search_Term LIKE '% krebs%' OR Search_Term LIKE '%krebs %'
THEN 'General cancer information'

WHEN REGEXP_CONTAINS(Search_Interest, r'Endometriose')
OR REGEXP_CONTAINS(Search_Term, r'endometr')
THEN "Endometriosis"

WHEN REGEXP_CONTAINS(Search_Interest, r'Menopause')
OR REGEXP_CONTAINS(Search_Interest, r'Östrogenmangel')
OR REGEXP_CONTAINS(Search_Interest, r'Hormonersatztherapie')
OR REGEXP_CONTAINS(Search_Term, r'menopause')
OR REGEXP_CONTAINS(Search_Term, r'wechseljah')
OR REGEXP_CONTAINS(Search_Term, r'östrogenmangel')
OR REGEXP_CONTAINS(Search_Term, r'welchseljahre')
THEN "Menopause"

WHEN REGEXP_CONTAINS(Search_Interest, r'Schwangerschaft')
OR REGEXP_CONTAINS(Search_Interest, r'Kinderwunsch')
OR REGEXP_CONTAINS(Search_Term, r'schwanger')
OR REGEXP_CONTAINS(Search_Term, r'eisprung')
OR REGEXP_CONTAINS(Search_Term, r'pregnan')
OR REGEXP_CONTAINS(Search_Term, r'ovul')
OR REGEXP_CONTAINS(Search_Term, r'fruchtb')
THEN "Pregnancy"

WHEN REGEXP_CONTAINS(Search_Interest, r'Polycystic ovary syndrom')
OR REGEXP_CONTAINS(Search_Term, r'pco')
OR REGEXP_CONTAINS(Search_Term, r'policys')
OR REGEXP_CONTAINS(Search_Term, r'polyzys')
OR REGEXP_CONTAINS(Search_Term, r'polit')
OR REGEXP_CONTAINS(Search_Term, r'%polycystic ovary syndrom')
THEN "PCOS"

WHEN REGEXP_CONTAINS(Search_Interest, r'PMS')
OR REGEXP_CONTAINS(Search_Interest, r'Menstruation')
OR REGEXP_CONTAINS(Search_Term, r'periode')
OR REGEXP_CONTAINS(Search_Term, r'regel')
OR REGEXP_CONTAINS(Search_Term, r'menstruation')
THEN "PMS"

ELSE "General Health" -- Mostly Nutrition and Hormone topics
END AS Category_3 --End Category 3

FROM dbtprojectnoah.dbt.src_unique_terms_interest
),

next_categories AS ( --Sub-query
SELECT *

,CASE --Start Category 4

WHEN Category_3 = "Breast Cancer" --Category MEDICAL THERAPY for Breast Cancer
AND REGEXP_CONTAINS(Search_Term, r'therapie')
AND NOT REGEXP_CONTAINS(Search_Term, r'adjuvant')
AND NOT REGEXP_CONTAINS(Search_Term, r'alternative')
THEN 'Medical Therapy'

WHEN Category_3 = "Breast Cancer" --Category DIAGNOSTIC for Breast Cancer
AND REGEXP_CONTAINS(Search_Term, r'diagnose')
AND REGEXP_CONTAINS(Search_Term, r'ultraschall')
THEN 'Diagnostic'

WHEN Category_3 = "Breast Cancer" --Category SURGERY for Breast Cancer
AND REGEXP_CONTAINS(Search_Term, r'ope')
THEN 'Surgery'

WHEN Category_3 = "Breast Cancer" --Category ADJUVANT THERAPY for Breast Cancer
AND REGEXP_CONTAINS(Search_Term, r'adjuvant')
THEN 'Adjuvant Therapy'

WHEN Category_3 = "Breast Cancer" --Category REHABILITATION for Breast Cancer
AND REGEXP_CONTAINS(Search_Term, r'rehab')
THEN 'Rehabilitation'

WHEN Category_3 = "Breast Cancer" --Category FOLLOWUP TREATMENT/CHECKUPS for Breast Cancer
AND REGEXP_CONTAINS(Search_Term, r'nach')
AND REGEXP_CONTAINS(Search_Term, r'therapie')
AND REGEXP_CONTAINS(Search_Term, r'ultraschall')
THEN 'Followup Treatment/Checkups'

WHEN Category_3 = "Breast Cancer" --Category ALTERNATIVE THERAPY for Breast Cancer
AND REGEXP_CONTAINS(Search_Term, r'alternative')
AND REGEXP_CONTAINS(Search_Term, r'brustkrebstherapie')
THEN 'Followup Treatment/Checkups'

WHEN Category_3 = "Breast Cancer" --Category GENERAL INFORMATION for Breast Cancer
THEN 'General Information'

END AS Category_4 --End Category 4

FROM categories
)

SELECT s.Search_Term, s.Category_1, s.Category_2, s.Category_3, d.Category_4
FROM categories s
JOIN next_categories d
USING (Search_Term)
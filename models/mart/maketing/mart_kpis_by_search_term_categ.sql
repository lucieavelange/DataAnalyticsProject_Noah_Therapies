-- Important to think of the order of the conditions --> priorities

WITH category_nb1 AS (
  SELECT *,
    CASE -- Start Category 1
      WHEN REGEXP_CONTAINS(Search_Term, r'männer') 
           OR REGEXP_CONTAINS(Search_Term, r'mann')
           OR REGEXP_CONTAINS(Search_Term, r'männ') 
      THEN "Men's Health"
      ELSE "Women's Health"
    END AS Category_1 -- End Category 1
  FROM {{ ref('src_unique_terms_interest') }}
),

category_nb2 AS ( -- Start Category 2
  SELECT *,
    CASE
      WHEN Category_1 = "Women's Health" THEN 
        CASE
          WHEN REGEXP_CONTAINS(Search_Interest, r'Adjuvante Chemotherapie') OR
               REGEXP_CONTAINS(Search_Interest, r'Mammakarzinom') OR 
               REGEXP_CONTAINS(Search_Interest, r'Chemotherapie') OR 
               REGEXP_CONTAINS(Search_Interest, r'Bestrahlung') OR 
               REGEXP_CONTAINS(Search_Interest, r'Body Mind therapie') OR 
               REGEXP_CONTAINS(Search_Interest, r'Hormontherapie') OR 
               REGEXP_CONTAINS(Search_Interest, r'Meditation') OR 
               REGEXP_CONTAINS(Search_Interest, r'Brustkrebs') OR
               REGEXP_CONTAINS(Search_Term, r'karzinom') OR 
               REGEXP_CONTAINS(Search_Term, r'brust') OR 
               REGEXP_CONTAINS(Search_Term, r'chemo') OR 
               REGEXP_CONTAINS(Search_Term, r'brest cancer') OR 
               REGEXP_CONTAINS(Search_Term, r'strahlung') OR 
               REGEXP_CONTAINS(Search_Term, r'körpertherapie') OR 
               REGEXP_CONTAINS(Search_Term, r'mind body') OR 
               REGEXP_CONTAINS(Search_Term, r'krebs')
          THEN "Cancer"
          ELSE "Non-Cancer"
        END
      ELSE ""
    END AS Category_2 -- End Category 2
  FROM category_nb1
),

category_nb3 AS ( -- Start Category 3
  SELECT *,
    CASE
      WHEN Category_2 = "Cancer" THEN 
        CASE
          WHEN REGEXP_CONTAINS(Search_Interest, r'Adjuvante Chemotherapie') OR 
               REGEXP_CONTAINS(Search_Interest, r'Mammakarzinom') OR 
               REGEXP_CONTAINS(Search_Interest, r'Chemotherapie') OR 
               REGEXP_CONTAINS(Search_Interest, r'Bestrahlung') OR 
               REGEXP_CONTAINS(Search_Interest, r'Body Mind therapie') OR 
               REGEXP_CONTAINS(Search_Interest, r'Meditation') OR 
               REGEXP_CONTAINS(Search_Interest, r'Hormontherapie') OR 
               REGEXP_CONTAINS(Search_Interest, r'Brustkrebs') OR 
               REGEXP_CONTAINS(Search_Term, r'karzinom') OR 
               REGEXP_CONTAINS(Search_Term, r'brust') OR 
               REGEXP_CONTAINS(Search_Term, r'chemo') OR 
               REGEXP_CONTAINS(Search_Term, r'brest cancer') OR 
               REGEXP_CONTAINS(Search_Term, r'strahlung') OR 
               REGEXP_CONTAINS(Search_Term, r'strahlen') OR 
               REGEXP_CONTAINS(Search_Term, r'körpertherapie') OR 
               REGEXP_CONTAINS(Search_Term, r'mind body') 
          THEN "Breast Cancer"
          
          WHEN REGEXP_CONTAINS(Search_Term, r'krebs') AND 
               NOT REGEXP_CONTAINS(Search_Term, r'brust') AND 
               Search_Term NOT LIKE '% krebs %' AND 
               Search_Term NOT LIKE '% krebs%' AND 
               Search_Term NOT LIKE '%krebs %' 
          THEN 'Other Cancers'
          
          WHEN Search_Term LIKE '% krebs %' OR 
               Search_Term LIKE '% krebs%' OR 
               Search_Term LIKE '%krebs %'
          THEN 'General cancer information'
          
        END
        
      WHEN Category_2 = "Non-Cancer" THEN 
        CASE 
          WHEN REGEXP_CONTAINS(Search_Interest, r'Endometriose') OR 
               REGEXP_CONTAINS(Search_Term, r'endometr') 
          THEN "Endometriosis"
          
          WHEN REGEXP_CONTAINS(Search_Interest, r'Menopause') OR 
               REGEXP_CONTAINS(Search_Interest, r'Östrogenmangel') OR 
               REGEXP_CONTAINS(Search_Interest, r'Hormonersatztherapie') OR 
               REGEXP_CONTAINS(Search_Term, r'menopause') OR 
               REGEXP_CONTAINS(Search_Term, r'wechseljah') OR 
               REGEXP_CONTAINS(Search_Term, r'östrogenmangel') OR 
               REGEXP_CONTAINS(Search_Term, r'welchseljahre') 
          THEN "Menopause"
          
          WHEN REGEXP_CONTAINS(Search_Interest, r'Schwangerschaft') OR 
               REGEXP_CONTAINS(Search_Interest, r'Kinderwunsch') OR 
               REGEXP_CONTAINS(Search_Term, r'schwanger') OR 
               REGEXP_CONTAINS(Search_Term, r'eisprung') OR 
               REGEXP_CONTAINS(Search_Term, r'pregnan') OR 
               REGEXP_CONTAINS(Search_Term, r'ovul') OR 
               REGEXP_CONTAINS(Search_Term, r'fruchtb') 
          THEN "Pregnancy"
          
          WHEN REGEXP_CONTAINS(Search_Interest, r'Polycystic ovary syndrom') OR 
               REGEXP_CONTAINS(Search_Term, r'pco') OR 
               REGEXP_CONTAINS(Search_Term, r'policys') OR 
               REGEXP_CONTAINS(Search_Term, r'polyzys') OR 
               REGEXP_CONTAINS(Search_Term, r'polit') OR 
               REGEXP_CONTAINS(Search_Term, r'%polycystic ovary syndrom') 
          THEN "PCOS"
          
          WHEN REGEXP_CONTAINS(Search_Interest, r'PMS') OR 
               REGEXP_CONTAINS(Search_Interest, r'Menstruation') OR 
               REGEXP_CONTAINS(Search_Term, r'periode') OR 
               REGEXP_CONTAINS(Search_Term, r'regel') OR 
               REGEXP_CONTAINS(Search_Term, r'menstruation') 
          THEN "PMS"
          
          ELSE "General Health" -- Mostly Nutrition and Hormone topics
        END
      ELSE ""
    END AS Category_3
  FROM category_nb2
),

joined AS (
  SELECT *
  FROM category_nb3
  JOIN {{ ref('src_search_terms') }}
  USING (Search_Term)
)

SELECT 
  Search_Term,
  Category_1,
  Category_2,
  Category_3,
  SUM(Impressions) AS impressions,
  SUM(Clicks) AS clicks,
  SUM(Conversions) AS conversions,
  ROUND(SUM(clicks)/SUM(impressions) * 100,2) AS CTR, -- click-through-rate = percentage of clicks
  ROUND(SUM(conversions)/SUM(impressions) * 100,2) AS conversion_rate
FROM joined
GROUP BY Search_Term, Category_1, Category_2, Category_3

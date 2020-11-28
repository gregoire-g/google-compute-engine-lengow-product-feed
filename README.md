# google-compute-engine-lengow-product-feed

The objective of this project is to download the latest product feed (source: Lengow) and upload one feed / luxe brand. To run the program you must issue the following command:
```
0 1 1 * * python ./lengow_feed_import/main.py
```

## Query example:
```
WITH product_feed as (
  SELECT ean, axe, sub_axe, revised_product_name, catalogue_price_without_vta
  FROM `loreal-france-luxe.product_feed.biotherm_product_feed`
  WHERE sap_sd_product_status IN ("02", "03", "04", "05") #02-03 = Starting, 03 = Launching, 05 = Cruising
  ),
  
lengow_feed as (
  SELECT id as ean, link, image_link FROM `loreal-france-luxe.lengow_feed.bio_lengow_flow_export`)

SELECT a.*, b.link, b.image_link 
FROM (
  SELECT * FROM product_feed) a
INNER JOIN (
  SELECT * FROM lengow_feed) b
ON a.ean = b.ean
```

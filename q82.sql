SELECT   i_item_id ,
         i_item_desc ,
         i_current_price
FROM     item,
         inventory,
         date_dim,
         store_sales
WHERE    i_current_price BETWEEN 34 AND      34+30
AND      inv_item_sk = i_item_sk
AND      d_date_sk=inv_date_sk
AND      d_date BETWEEN Cast('2002-04-30' AS DATE) AND      (
                  Cast('2002-04-30' AS DATE) + interval '60 days')
AND      i_manufact_id IN (840,109,510,582)
AND      inv_quantity_on_hand BETWEEN 100 AND      500
AND      ss_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id limit 100

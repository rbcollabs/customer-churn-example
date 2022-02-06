create or replace view <my_feature_store>.customer_churn_example.model_user_churn_predictions_final as (
  SELECT
   msno, ts,
   case
      when is_churn_true_prediction_score >= <churn_threshold> then True
      else False
   end as is_churn_prediction,
  FROM
    <my_feature_store>.customer_churn_example.model_user_churn_predictions
);

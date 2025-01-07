/* джойнимо таблиці, витягуємо необхідні колонки, рахуємо акаунти
*/
CREATE VIEW
Students.v_pikhulia_view_module_task AS
WITH accounts AS (
    SELECT
        s.date AS date,
        sp.country AS country,
        a.send_interval AS send_interval,
        a.is_verified AS is_verified,
        a.is_unsubscribed AS is_unsubscribed,
        COUNT(a.id) AS account_cnt
    FROM `data-analytics-mate.DA.account` a
    JOIN `data-analytics-mate.DA.account_session` acs
        ON a.id = acs.account_id
    JOIN `data-analytics-mate.DA.session_params` sp
        ON acs.ga_session_id = sp.ga_session_id
    JOIN `data-analytics-mate.DA.session` s
        ON s.ga_session_id = sp.ga_session_id
    GROUP BY s.date,
             sp.country,
             a.send_interval,
             a.is_verified,
             a.is_unsubscribed
/* рахуємо окремо акаунти по країнах, щоб не порушити групування по даті,країні,інтервалу,верифікації та підписках
*/            
),
total_country_acc AS(
SELECT
       *,
       SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt
FROM accounts      
 
),
/*джойнимо таблиці витягуємо колонки та групуємо все так само
*/
msg_by_acc AS (
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
        sp.country AS country,
        a.send_interval,
        a.is_verified,
        a.is_unsubscribed,
        COUNT(DISTINCT es.id_message) AS sent_msg,
        COUNT(DISTINCT eo.id_message) AS open_msg,
        COUNT(DISTINCT ev.id_message) AS visit_msg
    FROM `data-analytics-mate.DA.email_sent` es
    JOIN `data-analytics-mate.DA.account_session` acs
        ON es.id_account = acs.account_id
    JOIN `data-analytics-mate.DA.session` s
        ON s.ga_session_id = acs.ga_session_id
    LEFT JOIN `data-analytics-mate.DA.email_open` eo
        ON eo.id_message = es.id_message
    LEFT JOIN `data-analytics-mate.DA.email_visit` ev
        ON ev.id_message = es.id_message
    JOIN `data-analytics-mate.DA.session_params` sp
        ON sp.ga_session_id = s.ga_session_id
    JOIN `data-analytics-mate.DA.account` a
        ON acs.account_id = a.id
    GROUP BY
        date,
        sp.country,
        a.send_interval,
        a.is_verified,
        a.is_unsubscribed
),
/* окремо рахуємо загальну кількість листів по країнах
*/
msg_cnt AS (
    SELECT
        *,
        SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
    FROM msg_by_acc
),
/* залишаємо колонки по даті,країні,інтервалу,верифікації та підписках інші колонки змнюємо на 0 щоб потім агрегувати дані
*/


un_data AS (
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        account_cnt,
        0 AS sent_msg,
        0 AS open_msg,
        0 AS visit_msg,
        total_country_account_cnt,
        0 AS total_country_sent_cnt
    FROM total_country_acc


    UNION ALL


    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        0 AS account_cnt,
        sent_msg,
        open_msg,
        visit_msg,
        0 AS total_country_account_cnt,
        total_country_sent_cnt
    FROM msg_cnt
),
/* агрегуємо дані щоб прибрати 0
*/
final_agr_data AS(
SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(account_cnt) AS account_cnt,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(visit_msg) AS visit_msg,
    SUM(total_country_account_cnt) AS total_country_account_cnt,
    SUM(total_country_sent_cnt) AS total_country_sent_cnt,
   
FROM un_data
GROUP BY 1, 2, 3, 4, 5
),
/* окремо рахуємо ранки країн, бо після сумування в нас будуть неадекватні значення
*/
final_rank_data AS(
SELECT *,
       DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account,
       DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent
FROM final_agr_data
)


SELECT *
FROM final_rank_data
WHERE rank_total_country_account <= 10 OR rank_total_country_sent <= 10
ORDER BY country;

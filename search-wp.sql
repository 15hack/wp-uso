select TT1.table_schema, wp_posts from (
  select table_schema, table_name wp_user from (
    select table_schema, table_name, count(*) N
    from information_schema.COLUMNS
    where
      column_name in ('ID', 'user_email', 'user_pass', 'user_nicename') and
      table_name like '%user%'
    group by table_schema, table_name
  ) T1 where T1.N=4
) TT1
JOIN
(
  select table_schema, table_name wp_usermeta from (
    select table_schema, table_name, count(*) N
    from information_schema.COLUMNS
    where
      column_name in ('umeta_id', 'user_id', 'meta_key', 'meta_value') and
      table_name like '%usermeta%'
    group by table_schema, table_name
  ) T2 where T2.N=4
) TT2
JOIN
(
  select table_schema, table_name wp_posts from (
    select table_schema, table_name, count(*) N
    from information_schema.COLUMNS
    where
      column_name in ('ID', 'post_author', 'post_status', 'guid', 'post_type') and
      table_name like '%_posts'
    group by table_schema, table_name
  ) T3 where T3.N=5
) TT3
ON TT1.table_schema = TT2.table_schema and TT1.table_schema = TT3.table_schema
order by table_schema, wp_posts;

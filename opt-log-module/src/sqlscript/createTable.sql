-- drop table F_OPT_LOG cascade constraints;

-- Create table
create table F_OPT_LOG
(
  log_id         VARCHAR2(32) not null,
  log_level      VARCHAR2(2) not null,
  user_code      VARCHAR2(8) not null,
  opt_time       DATE not null,
  opt_content    VARCHAR2(1000) not null,
  new_value      CLOB,
  old_value      CLOB,
  opt_id         VARCHAR2(64) not null,
  opt_method     VARCHAR2(64),
  opt_tag        VARCHAR2(200),
  correlation_id VARCHAR2(32),
  unit_code      VARCHAR2(32),
  constraint PK_F_OPT_LOG primary key (LOG_ID)
);


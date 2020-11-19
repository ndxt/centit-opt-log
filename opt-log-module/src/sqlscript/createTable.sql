-- drop table F_OPT_LOG cascade constraints;

-- Create table
create table F_OPT_LOG
(
  LOG_ID         VARCHAR(32) NOT NULL,
  LOG_LEVEL      VARCHAR(2) NOT NULL,
  USER_CODE      VARCHAR(8) NOT NULL,
  OPT_TIME       DATE NOT NULL,
  OPT_CONTENT    VARCHAR(1000) NOT NULL,
  NEW_VALUE      CLOB, -- text, 
  OLD_VALUE      CLOB, -- text,
  OPT_ID         VARCHAR(64) NOT NULL,
  OPT_METHOD     VARCHAR(64),
  OPT_TAG        VARCHAR(200),
  CORRELATION_ID VARCHAR(32),
  UNIT_CODE      VARCHAR(32),
  constraint PK_F_OPT_LOG primary key (LOG_ID)
);


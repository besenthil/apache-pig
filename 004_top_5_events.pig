-- Load events data from hdfs and parse records into columns
source_events = LOAD '/user/data/*.export.CSV' USING PigStorage('\t') AS (GLOBALEVENTID:chararray,	SQLDATE:chararray,	MonthYear:chararray,	Year:chararray,	FractionDate:chararray,	Actor1Code:chararray,	Actor1Name:chararray,	Actor1CountryCode:chararray,	Actor1KnownGroupCode:chararray,	Actor1EthnicCode:chararray,	Actor1Religion1Code:chararray,	Actor1Religion2Code:chararray,	Actor1Type1Code:chararray,	Actor1Type2Code:chararray,	Actor1Type3Code:chararray,	Actor2Code:chararray,	Actor2Name:chararray,	Actor2CountryCode:chararray,	Actor2KnownGroupCode:chararray,	Actor2EthnicCode:chararray,	Actor2Religion1Code:chararray,	Actor2Religion2Code:chararray,	Actor2Type1Code:chararray,	Actor2Type2Code:chararray,	Actor2Type3Code:chararray,	IsRootEvent:chararray,	EventCode:chararray,	EventBaseCode:chararray,	EventRootCode:chararray,	QuadClass:int,	GoldsteinScale:double,	NumMentions:int,	NumSources:int,	NumArticles:int,	AvgTone:double,	Actor1Geo_Type:chararray,	Actor1Geo_FullName:chararray,	Actor1Geo_CountryCode:chararray,	Actor1Geo_ADM1Code:chararray,	Actor1Geo_Lat:double,	Actor1Geo_Long:double,	Actor1Geo_FeatureID:int,	Actor2Geo_Type:chararray,	Actor2Geo_FullName:chararray,	Actor2Geo_CountryCode:chararray,	Actor2Geo_ADM1Code:chararray,	Actor2Geo_Lat:double,	Actor2Geo_Long:double,	Actor2Geo_FeatureID:int,	ActionGeo_Type:chararray,	ActionGeo_FullName:chararray,	ActionGeo_CountryCode:chararray,	ActionGeo_ADM1Code:chararray,	ActionGeo_Lat:double,	ActionGeo_Long:double,	ActionGeo_FeatureID:int,	DATEADDED:int,	SOURCEURL:chararray);	

-- Tokenize the URL
tokenized_underscore = FOREACH source_events generate GLOBALEVENTID, FLATTEN(STRSPLIT(SOURCEURL,'-',10000)) as word;

-- Tokenize the URL
tokenized_slash = FOREACH tokenized generate GLOBALEVENTID, FLATTEN(STRSPLIT(word,'/',10000)) as word;

-- Group the words
--grouped = GROUP tokenized_slash by word;

-- dump
DUMP tokenized_slash;

-- Store the output into HDFS
--STORE output_final INTO '/user/data/FINAL_OUTPUT.CSV';

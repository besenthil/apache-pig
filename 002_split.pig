
-- declare variables
%declare source_file_path '/home/senthil/Downloads';

-- Load events data from hdfs and parse records into columns
source_events = LOAD '$source_file_path/*.export.CSV' USING PigStorage('\t') AS (GLOBALEVENTID:chararray,	SQLDATE:chararray,	MonthYear:chararray,	Year:chararray,	FractionDate:chararray,	Actor1Code:chararray,	Actor1Name:chararray,	Actor1CountryCode:chararray,	Actor1KnownGroupCode:chararray,	Actor1EthnicCode:chararray,	Actor1Religion1Code:chararray,	Actor1Religion2Code:chararray,	Actor1Type1Code:chararray,	Actor1Type2Code:chararray,	Actor1Type3Code:chararray,	Actor2Code:chararray,	Actor2Name:chararray,	Actor2CountryCode:chararray,	Actor2KnownGroupCode:chararray,	Actor2EthnicCode:chararray,	Actor2Religion1Code:chararray,	Actor2Religion2Code:chararray,	Actor2Type1Code:chararray,	Actor2Type2Code:chararray,	Actor2Type3Code:chararray,	IsRootEvent:chararray,	EventCode:chararray,	EventBaseCode:chararray,	EventRootCode:chararray,	QuadClass:int,	GoldsteinScale:double,	NumMentions:int,	NumSources:int,	NumArticles:int,	AvgTone:double,	Actor1Geo_Type:chararray,	Actor1Geo_FullName:chararray,	Actor1Geo_CountryCode:chararray,	Actor1Geo_ADM1Code:chararray,	Actor1Geo_Lat:double,	Actor1Geo_Long:double,	Actor1Geo_FeatureID:int,	Actor2Geo_Type:chararray,	Actor2Geo_FullName:chararray,	Actor2Geo_CountryCode:chararray,	Actor2Geo_ADM1Code:chararray,	Actor2Geo_Lat:double,	Actor2Geo_Long:double,	Actor2Geo_FeatureID:int,	ActionGeo_Type:chararray,	ActionGeo_FullName:chararray,	ActionGeo_CountryCode:chararray,	ActionGeo_ADM1Code:chararray,	ActionGeo_Lat:double,	ActionGeo_Long:double,	ActionGeo_FeatureID:int,	DATEADDED:int,	SOURCEURL:chararray);	

-- Remove bad records with issues in country
SPLIT source_events INTO good_records_with_country_is_not_null IF ActionGeo_CountryCode is not null, 
bad_records_with_country_is_null OTHERWISE;

-- Remove bad records that have a old dated entry
SPLIT good_records_with_country_is_not_null INTO good_records_with_new_recirds IF Year >= '2016', 
bad_records_with_old_dates OTHERWISE;

-- Store the bad records in separate files
STORE bad_records_with_country_is_null INTO '$source_file_path/BAD_RECORDS_WITH_COUNTRY_NULL.CSV';
STORE bad_records_with_old_dates INTO '$source_file_path/BAD_RECORDS_WITH_OLD_DATES.CSV';

/* inputs */
/*
%let libname = terumo;
%let worklib = terumo;
%let profstart=1;
%let profmax=100;
%let freqmin=1;
%let freqmax=50000000;
%let id_vars = first_name, last_name; * should be comma-separated ;
*/
/* end inputs */

/* call with %CHARACTERIZATION(ods,fds); where ods=original dataset and fds=final dataset names */


%macro getvar(numobs,startlim,endlim) / minoperator mindelimiter=',';
	%if %eval(&endlim+&startlim-1) lt %eval(&numobs) %then %let endctr = %eval(&startlim+&endlim);
	%else %let endctr = &numobs;
	%do i = &startlim %to &endctr;
		*check if ID variable, if so, skip over;
		data _null_; pointer=&i.; set outlist point=pointer;
			CALL SYMPUT('idcheck',trim(name));
			stop;
		run;
		
		%put &idcheck;

		%if %upcase(&idcheck) in (&id_vars.) %then %do;
			%put ID Variable &idcheck skipped.;
			%goto exit;
		%end;

		*if not ID variable, include;
		DATA _null_;
			ATTRIB min_var FORMAT=$200. max_var FORMAT=$200. miss_var FORMAT=$200. distinct_var FORMAT=$200.
				nomiss_var FORMAT=$200. search_name FORMAT=$35. dataset FORMAT=$42. vlength FORMAT=$10. orig_format FORMAT=$10.
				path_name FORMAT=$100.;
			;
			pointer=&i.;
			SET outlist point=pointer;
			vlength=length;
			search_name="'"||TRIM(name)||"'N";
			path_name = pathname(libname);
			dataset = TRIM(libname)||".'"||TRIM(memname)||"'N";
			if FORMAT=:'DATE' or FORMAT=:'JUL' or FORMAT=:'MM' or FORMAT=:'DD' or FORMAT=:'DAY' or FORMAT=:'MON' or 
			  FORMAT=:'YEAR' or FORMAT=:'WORDD' or FORMAT=:'EURD' or FORMAT=:'WEEK' then do;
				FORMAT='MMDDYY10.';
				orig_format='DATE';
			end;
			else if FORMAT=:'TIME' or FORMAT=:'HHMM' then do;
				FORMAT='TIME20.';
				orig_format='TIME';
			end;
			else if type = 'num' then do;
				orig_FORMAT=TRIM(UPCASE(type))||compress(vlength);
				FORMAT='20.';
			end;
			else if type = 'char' then do;
				FORMAT='$100.';
				orig_FORMAT=TRIM(UPCASE(type))||compress(vlength);
			end;
			MIN_VAR = 'create table temprange as select MIN('||TRIM(search_name)||') as min_value1';
			max_var = ',MAX('||TRIM(search_name)||') as max_value1';
			miss_var = ',NMISS('||TRIM(search_name)||') as miss_value';
			distinct_var = ',COUNT(DISTINCT('||TRIM(search_name)||')) as distinct_values';
			nomiss_var = ',COUNT('||TRIM(search_name)||') as nmiss_value from '||TRIM(dataset)||';';
			CALL SYMPUT('SQL1',TRIM(MIN_VAR));
			CALL SYMPUT('SQL2',TRIM(MAX_VAR));
			CALL SYMPUT('SQL3',TRIM(MISS_VAR));
			CALL SYMPUT('SQL4',TRIM(DISTINCT_VAR));
			CALL SYMPUT('SQL5',TRIM(NOMISS_VAR));
			CALL SYMPUT('var',TRIM(name));
			CALL SYMPUT('dataset',TRIM(dataset));
			CALL SYMPUT('var_n', QUOTE(name)||"n");
			CALL SYMPUT('type',TRIM(type));
			CALL SYMPUT('label',label);
			CALL SYMPUT('format',format);
			CALL SYMPUT('orig_format',orig_format);
			CALL SYMPUT('path_name',path_name);
			STOP;
		run;

		proc sql; &sql1 &sql2 &sql3 &sql4 &sql5 quit;

		data temprange;
			ATTRIB dataset format=$41. libpath format=$100. variable format=$32. label format=$256. format format=$31. 
				db_format format=$31. min_value format=$100. max_value format=$100. ;
			set temprange;
			dataset=TRIM("&libname..&memname");
			variable = TRIM("&var");
			format=TRIM("&format");
			db_format=TRIM("&orig_format");
			type=TRIM("&type");
			label=TRIM("&label");
			libpath=TRIM("&path_name");
			if type='num' then do;
				min_value=compress(put(min_value1,&format.));
				max_value=compress(put(max_value1,&format.));
			end;
			else do;
				min_value=min_value1;
				max_value=max_value1; 
			end;
			DROP min_value1 max_value1 type;
		run;

		proc append base=all_stats data=temprange FORCE; run;
		%exit:
	%end;
%mend getvar;

%macro dofreq;
	%do i = 1 %to &numobs.;
		data _null_;
			attrib freqname format=$40.;
			pointer=&i.;
			set SET_FREQS point=pointer;
			if format = '' or format='$' then format='$32.';
			else format=trim(format);
			freqname="&libname..'&memname.'n";
			call symput('var', variable);
			call symput('var_n', quote(variable) || "n");
			call symput('format', format);
			call symput('datast', dataset);
			stop;
		run;

		proc freq data=&datast. noprint;
			tables &var_n. / missing out=tempfreq; 
		run;

		data tempfreq; set tempfreq;
			attrib value format=$32. ;
			dataset = "&datast";
			variable = "&var";
			value = trim(put(&var.,&format.));
		run;
		proc append base=storefreqs data=tempfreq FORCE; run;
	%end;
%mend dofreq; 

%macro characterization(ods,fds); 
	%let memname=&ods ;
	data all_stats (label="Ranges for All Variables");
		attrib dataset format=$41. variable format=$32. label format=$256. format format=$31. min_value format=$100.
			max_value format=$100. miss_value format=comma12. distinct_values format=comma12. nmiss_value format=comma12. ;
		label min_value = 'Minimum Value' max_value = 'Maximum Value' miss_value = '# of Missing Values'
			distinct_values = '# of Distinct Values' nmiss_value = '# of Non-missing Values';
		stop;
	run;

	data storefreqs (label="Frequency Counts for Selected Variables");
		attrib dataset format=$41. variable format=$32. value format=$100. count format=8. percent format=6.2; 
		label count='Frequency Count' Percent='Percent of Total Frequency';
		stop;
	run;

	proc sql;
		create table outlist as 
		select * from
			(select * from dictionary.columns where memtype = 'DATA' and UPCASE(LIBNAME) = UPCASE("&libname") and 
			UPCASE(memname) = upcase("&ods"))
		order by name;
	quit;

	%getvar(&sqlobs, &profstart, &profmax);

	data set_freqs; set all_stats;
		where distinct_values between &freqmin and &freqmax ;
		call symput('numobs',put(_n_, 12.));
	run;

	%dofreq;

	*save datasets;
	data &worklib..freqs_&fds ; set storefreqs; run;
	data &worklib..stats_&fds ; set all_stats; run;

%mend characterization;

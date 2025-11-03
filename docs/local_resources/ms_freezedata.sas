****************************************************************************************************
*                                           PROGRAM OVERVIEW
****************************************************************************************************
*--------------------------------------------------------------------------------------------------
* PURPOSE:
*   This program creates a subset of a dataset containing patient level data.
*
*  Program inputs:
* 	-SAS dataset with patient level data
*   -SAS dataset with patient list (PATID) for subset creation
*
*  Program outputs:
*	-SAS dataset with subset of patient level data
*
*  PARAMETERS:
*	  INFILE    		= SAS dataset with patient-level data
*   PATLISTINFILE 	= SAS dataset with list of patients
*   OUTFILE     	= SAS dataset output file with subset of patient level data
***************************************************************************************************;

%macro MS_FREEZEDATA(INFILE=,PATLISTINFILE=,OUTFILE=);

%put ***** MACRO called: MS_FREEZEDATA v1.1 ******;

  proc sort nodupkey data=&PATLISTINFILE.(keep=PATID)
             out=_tempPatList;
  by PatId;
  run;

  data &OUTFILE.;
  set _tempPatList point = _n_;
  declare hash tt (hashexp:16, dataset:"_tempPatList");
  tt.definekey('PATID');
  tt.definedata(ALL: 'YES');
  tt.definedone();

  do until(eof1);
    set &INFILE. end=eof1;
    if tt.find()=0 then output;
  end;
  stop;
  run;

  proc datasets library = work nolist nowarn nodetails;
  delete _tempPatList;
  quit;

%put ******** END OF MACRO: MS_FREEZEDATA v1.1 ********;

%mend MS_FREEZEDATA;

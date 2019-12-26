libname Brian '/sas/bu_content/csoda/eg_projects/';

%macro PowerContents(input=,output=,mode=);

	%if %lowcase(&mode)=list %then
		%do;
			%let N_=;

			data _null_;
				set &input.;
				call symputx('lib_'||left(put(_n_,8.)),libname);
				call symputx('table_'||left(put(_n_,8.)),TableName);
				call symputx('var_'||left(put(_n_,8.)),VariableName);
				call symputx('N_',left(put(_n_,8.)));
			run;

			%if %length(&N_.)^=0 %then
				%do _x_=1 %to &N_.;

					proc contents data=&&lib_&_x_...&&table_&_x_ (keep=&&var_&_x_) noprint DETAILS
						out=_tmp_(where=(memtype="DATA") keep=libname memname name type label length varnum format nobs memtype
						rename=(varnum=No memname=TableName name=VariableName));
					run;

					%if %sysfunc(mod(&_x_.,50))=0 %then
						%do;
							DM 'clear log';
						%end;

					%if &_x_.=1 %then
						%do;

							data _variable_infor_;
								set _tmp_;
							run;

						%end;
					%else
						%do;

							data _variable_infor_;
								set _variable_infor_ _tmp_;
							run;

						%end;
				%end;
		%end;
	%else
		%do;

			proc contents data=&input. noprint DETAILS
				out=_variable_infor_(where=(memtype="DATA") keep=libname memname name type label length varnum format nobs memtype
				rename=(varnum=No memname=TableName name=VariableName));
			run;

		%end;

	proc sort data=_variable_infor_;
		by no;
	run;

	quit;

	data _variable_infor_;
		retain no;
		set _variable_infor_;
		libname=propcase(libname);
		tablename=propcase(tablename);
		variablename=propcase(variablename);
	run;

	%let N_infor=;

	data _null_;
		set _variable_infor_;
		call symputx('lib_infor'||left(put(_n_,8.)),libname);
		call symputx('table_infor'||left(put(_n_,8.)),TableName);
		call symputx('var_infor'||left(put(_n_,8.)),VariableName);
		call symputx('var_type'||left(put(_n_,8.)),type);
		call symputx('var_format'||left(put(_n_,8.)),format);
		call symputx('N_infor',left(put(_n_,8.)));
	run;

	%if %length(&N_infor.)^=0 %then
		%do;
			%do _i_=1 %to &N_infor.;
/*
				proc freq data=&&lib_infor&_i_...&&table_infor&_i_(keep=&&var_infor&_i_) noprint;
					table &&var_infor&_i_/ missing out=_var_freq_;
				run;

				proc sql noprint;
					create table _var_count_ as
						select
							missing(&&var_infor&_i_) as Missing,
							sum(count) as N_obs,
							count(*) as  N_value
						from _var_freq_
							group by Missing
								order by Missing
					;
				quit;
*/
				proc sql noprint;
					create table _var_count_ as
					select
						count(distinct &&var_infor&_i_) as N_dist_value,
						count(*) as N_Non_missing
						from &&lib_infor&_i_...&&table_infor&_i_(keep=&&var_infor&_i_)
						where &&var_infor&_i_ is not null
					;
				quit;

				data _var_count2_;
					set _var_count_;
					format lib_name $32.;
					format table_name $32.;
					format variable_name $32.;
					Lib_name="&&lib_infor&_i_";
					Table_name="&&table_infor&_i_";
					Variable_name="&&var_infor&_i_";
					Lib_name=propcase(Lib_name);
					Table_name=propcase(Table_name);
					Variable_name=propcase(variable_name);
					format Nmax 8.3;
					format Nmin 8.3;
					format integer_dummy 8.;
					Nmax=.;
					Nmin=.;
					integer_dummy=.;
				run;

				%let integer_check=0;

				%if &&var_type&_i_=1 %then %if %length(&&var_format&_i_)=0 %then
					%let integer_check=1;
				%else
					%if  %substr(&&var_format&_i_,1,1)=8 or
					%substr(&&var_format&_i_,1,1)=4 or
					%substr(&&var_format&_i_,1,1)=7 or
					%substr(&&var_format&_i_,1,1)=6 or
					%substr(&&var_format&_i_,1,1)=5 or
					%substr(&&var_format&_i_,1,1)=3
					%then
					%let integer_check=1;

				%if &integer_check.=1 %then
					%do;

						proc sql noprint;
							create table _var_num_type_ as
								select
									max(&&var_infor&_i_) as Nmax format=8.3,
									min(&&var_infor&_i_) as Nmin format=8.3,
									min(&&var_infor&_i_=round(&&var_infor&_i_)) as integer_dummy
								from &&lib_infor&_i_...&&table_infor&_i_(keep=&&var_infor&_i_)
							;
						quit;

						data _var_count2_;
							merge _var_count2_(drop=Nmax Nmin integer_dummy) _var_num_type_;
						run;

						proc sql noprint;
							drop table _var_num_type_;
					%end;

				%if &_i_.=1 %then
					%do;

						data _tmp_;
							set _var_count2_;
						run;

					%end;
				%else
					%do;

						data _tmp_;
							set _tmp_ _var_count2_;
						run;

					%end;
					/*
				%if %sysfunc(mod(&_i_.,50))=0 %then
					%do;
						DM 'clear log';
					%end;
					*/
			%end;

			proc sql noprint;
				create table &output. as
					select A.*,
						B.N_Non_missing,
						A.Nobs-B.N_Non_missing as N_missing,
						B.N_Non_missing/A.Nobs as Percent_Non_missing format=percent8.2,
						(A.Nobs-B.N_Non_missing)/A.Nobs as Percent_missing format=percent8.2,
						B.N_dist_value,
						B.Nmax label="Max(Numberical Variable)",
						B.Nmin label="Min(Numberical Variable)",
						B.integer_dummy label="integer"
					from _variable_infor_ A
						left join _tmp_ B
							on propcase(A.libname)=B.lib_name and
							propcase(A.tablename)=B.table_name and
							propcase(A.variablename)=B.variable_name
				;
			quit;

			proc sort data=&output.;
				by no;
			run;

			quit;

			proc sql noprint;
				drop table _var_count_, _var_count2_, _tmp_,_variable_infor_;
			quit;

		%end;
%mend;


 

%macro uni_graphics(task_No=,library_name=,table_name=,variable_name=,variable_format=,Graphic_type=,N_deci=);

proc template;
define statgraph graph.histoboxtmp;
dynamic _varplot_;
begingraph / designheight=467 designwidth=591;
layout lattice _id='lattice' / columndatarange=union columngutter=10 rowdatarange=data rowgutter=10 rowweights=(0.8099999999999999 0.19000000000000006 ) rows=2;
         layout overlay _id='overlay' / xaxisopts=(linearopts=());
            histogram _id='histogram' _varplot_ / binaxis=false name='histogram';
            densityplot _id='Normal' _varplot_ / normal() name='Normal';
            densityplot _id='Kernel' _varplot_ / kernel() lineattrs=GraphData2(thickness=2) name='Kernel';
            discretelegend _id='legend2' 'Normal' 'Kernel'  / across=1 border=true displayclipped=true halign=right location=inside opaque=false order=rowmajor valign=top;
         endlayout;
         layout overlay _id='overlay2' /;
            boxplot _id='box(h)' y=_varplot_ / name='box(h)' orient=horizontal;
         endlayout;
         columnaxes;
            columnaxis _id='columnaxis' /;
         endcolumnaxes;
endlayout;
endgraph;
end;
run;
      %if "&Graphic_type"="Summary Statistics - Numerical Variable" %then
            %do;
                  %if "%upcase(&variable_format.)"="NUM" %then
                        %do;
                              proc means data=&library_name..&table_name. Nmiss N mean std min Q1 median Q3 max SKEW maxdec=&N_deci.;
                                    var &variable_name.;
                              run;
                              quit;
                        %end;
                  %else
                        %do;
                              %if "%upcase(&variable_format.)"="DATE" %then %let ffmt=date9%str(.); 
                              %if "%upcase(&variable_format.)"="TIME" %then %let ffmt=time8%str(.); 
                              %if "%upcase(&variable_format.)"="DATETIME" %then %let ffmt=datetime18%str(.); 

                              proc tabulate data=&library_name..&table_name.;
                                    var &variable_name.;
                                    table  &variable_name., n nmiss;
                                    table  &variable_name.*f=%unquote(&ffmt.), min max;
                              run;

                        %end;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Univariate &task_No.: &variable_name.(Summary Statistics)";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;

 

      %if "&Graphic_type"="Histogram" %then
            %do;
                  proc sgplot data=&library_name..&table_name.;
                        histogram &variable_name./ showbins scale=count;
                        density &variable_name.;
                  run;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Univariate &task_No.: &variable_name.(Histogram)";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;

 

      %if "&Graphic_type"="Box Plot" %then
            %do;
                  proc sgplot data=&library_name..&table_name.;
                        hbox &variable_name.;
                  run;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Univariate &task_No.: &variable_name.(Horizontal Box Plot)";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;

       %if "&Graphic_type"="Histo-Box Plot" %then
            %do;

					proc sgrender data=&library_name..&table_name. template=graph.histoboxtmp;
					  dynamic _varplot_="&variable_name." 
					;
					run;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Univariate &task_No.: &variable_name.(Histogram and Box Plot)";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;


      %if "&Graphic_type"="Frequecy Table" %then
            %do;
                  proc freq data=&library_name..&table_name.;
                        table &variable_name./PLOTS=none;
                  run;
                  quit;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Univariate &task_No.: &variable_name.(Frequency Analysis)";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */

            %end;

      %if "&Graphic_type"="Vertical Bar" %then
            %do;
                  proc sgplot data=&library_name..&table_name.;
                        vbar &variable_name.;
                  run;
                  ods pdf text="^S={%unquote(&pdffontformat.)}Univariate &task_No.: &variable_name.(Vertical Bar Plot)";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;

      %if "&Graphic_type"="Horizontal Bar" %then
            %do;
                  proc sgplot data=&library_name..&table_name.;
                        hbar &variable_name.;
                  run;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Univariate &task_No.: &variable_name.(Horizontal Bar Plot)";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;
%mend;

 

/*if variable type is different, Var1 is num, and var2 is char*/

 

%macro Bi_graphics(task_No=,lib_name=,table_name=,variable_name1=,variable_name2=,variable_name3=,Graphic_type=,N_deci=,N_sample=);

	proc template;
	define statgraph graph.scattertmp;
	dynamic _AGE_AT_INJURY _TIME_TO_SURGERY _AGE_AT_INJURY2 _TIME_TO_SURGERY2;
	mvar  _title_set;
	begingraph;
	entrytitle _id='title2' halign=center '' /;
	layout lattice _id='lattice' / columndatarange=union columngutter=10 columnweights=(0.7418772563176895 0.2581227436823105 ) columns=2 rowdatarange=union rowgutter=10 rowweights=(0.3191489361702128 0.6808510638297872 ) rows=2;
	         layout overlay _id='overlay2' / xaxisopts=(linearopts=());
	            histogram _id='histogram' _AGE_AT_INJURY2 / binaxis=false name='histogram';
	         endlayout;
	         layout overlay _id='overlay4' /;
	            entry _id='dropsite4' halign=center '(drop a plot here...)' / valign=center;
	         endlayout;
	         layout overlay _id='overlay' /;
	            scatterplot _id='scatter' x=_AGE_AT_INJURY y=_TIME_TO_SURGERY / datatransparency=0.8 markerattrs=(symbol=CIRCLEFILLED size=9) name='scatter';
	            regressionplot _id='regression' x=_AGE_AT_INJURY y=_TIME_TO_SURGERY / lineattrs=GraphFit2 name='regression';
	            loessplot _id='loess' x=_AGE_AT_INJURY y=_TIME_TO_SURGERY / name='loess';
	         endlayout;
	         layout overlay _id='overlay3' / xaxisopts=(linearopts=());
	            histogram _id='histogram(h)' _TIME_TO_SURGERY2 / binaxis=false name='histogram(h)' orient=horizontal;
	         endlayout;
	         rowaxes;
	            rowaxis _id='rowaxis' /;
	            rowaxis _id='rowaxis2' / griddisplay=ON;
	         endrowaxes;
	         columnaxes;
	            columnaxis _id='columnaxis' / griddisplay=ON linearopts=();
	            columnaxis _id='columnaxis2' /;
	         endcolumnaxes;
	endlayout;
	endgraph;
	end;
	run;

      %if "&lib_name."="" %then %let ttt_name=&table_name.;
      %else %let ttt_name=&lib_name..&table_name.;

      %if %length(&lib_name.)=0 %then %let libname=work;
      %if %lowcase(&lib_name.)=work %then %let out_ltname=&table_name.;
      %else %let out_ltname=&lib_name..&table_name.;

      %if "&Graphic_type"="Summary Statistics By Group" %then
            %do;

                  proc means data=&ttt_name. PRINTALLTYPES Nmiss N mean std min Q1 median Q3 max SKEW KURT maxdec=&N_deci.;
                        class &variable_name2.;
                        var &variable_name1.;
                  run;
                  quit;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Bivariate &task_No.: Summary Statistics, &variable_name1. Group by &variable_name2.";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;

      %if "&Graphic_type"="Box Plot By Group" %then
            %do;
                  proc sgplot data=&ttt_name.;
                        hbox &variable_name1./category=&variable_name2. missing;
                  run;

                  ods select ModelANOVA ;/*BoxPlot;*/
                  ods output ModelANOVA=_anova_tmp;
                  proc anova data=&ttt_name.;
                        class &variable_name2.;
                        model &variable_name1.=&variable_name2.;
                  run;
                  quit;

                  ods select all;
                  ods output close;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Bivariate &task_No.: Horizontal Box Plot, &variable_name1. group by &variable_name2.";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */

                  data _anova_tmp;
                        format Task_id $50.;
                        format Table $60.;
                        format Dependent $32.;
                        format Group $32.;
                        set _anova_tmp(drop=HypothesisType Dependent Source);
                        task_id="&task_No.";
                        table="&out_ltname.";
                        Dependent="&variable_name1.";
                        Group="&variable_name2.";
                        label task_id="Task ID";
                        label ProbF="P Value";
                  run;

                  %if %sysfunc(exist(_Anova_)) %then
                        %do;
                              data _Anova_;
                                    set _Anova_ _Anova_tmp;
                              run;
                        %end;
                  %else
                        %do;
                              data _Anova_;
                                    set _Anova_tmp;
                              run;
                        %end;

                  proc sql noprint; drop table _Anova_tmp; quit;
            %end;

      %if "&Graphic_type"="Mean Plot Chart by Group" %then
            %do;
				proc sgplot data=&ttt_name.;
				      dot &variable_name2./response=&variable_name1. STAT=MEAN LIMITSTAT=STDDEV;
				run;
                ods pdf text="^S={%unquote(&pdffontformat.)}Bivariate &task_No.: Mean Plot, &variable_name1. by &variable_name2.";
                ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;

      %if "&Graphic_type"="2*2 Frequency Analysis" %then

            %do;
                  ods output chisq=_chisq_tmp;
                  proc freq data=&ttt_name.;
                        table &variable_name1.*&variable_name2./PLOTS=none chisq norow nocol nopercent;
                  run;
                  quit;
                  ods output close;
 				  %if %sysfunc(exist(_chisq_tmp)) %then
				  %do;
	                  data _chisq_tmp;
	                        format Task_id $50.;
	                        format Table $60.;
	                        format Variable1 $32.;
	                        format Variable2 $32.;
	                        format DoF 8.;
	                        format Chi_SQ 10.4;
	                        format P_Value PVALUE8.4;
	                        set _chisq_tmp(drop=table);
	                        if statistic="Chi-Square";
	                        DoF=DF;
	                        Chi_SQ=value;
	                        P_value=Prob;

	                        task_id="&task_No.";
	                        table="&out_ltname.";
	                        variable1="&variable_name1.";
	                        variable2="&variable_name2.";
	                        label task_id="Task ID";
	                        label DoF="Degree of Freedom";
	                        label Chi_SQ="Chi-Square Statistics";
	                        label P_Value="P Value";

	                        keep Task_id table Variable1 Variable2 Dof Chi_SQ P_value; 
	                  run;

	                  %if %sysfunc(exist(_ChiSQ_)) %then
	                        %do;
	                              data _ChiSQ_;
	                                    set _ChiSQ_ _chisq_tmp;
	                              run;
	                        %end;
	                  %else
	                        %do;
	                              data _ChiSQ_;
	                                    set _chisq_tmp;
	                              run;
	                        %end;

	                  proc sql noprint; drop table _chisq_tmp; quit;
				  %end;
                  ods pdf text="^S={%unquote(&pdffontformat.)}Bivariate &task_No.: 2*2 Frequency Analysis, &variable_name1. VS &variable_name2.";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;

      %if "&Graphic_type"="Vertical Bar Chart by Group" %then
            %do;
                  proc sgplot data=&ttt_name.;
                        vbar &variable_name1./group=&variable_name2.;
                  run;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Bivariate &task_No.: Vertical Stack Bar Plot, &variable_name1. by &variable_name2.";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;

      %if "&Graphic_type"="Horizontal Bar Chart by Group" %then
            %do;
                  proc sgplot data=&ttt_name.;
                        hbar &variable_name1./group=&variable_name2.;
                  run;

                  ods pdf text="^S={%unquote(&pdffontformat.)}Bivariate &task_No.: Horizontal Stack Bar Plot, &variable_name1. by &variable_name2.";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */
            %end;




      %if %index(&Graphic_type.,Scatter)>0 %then
            %do;
                  %if %length(&variable_name3.)^=0 %then
                        %let group_by_option=%str(/) group %str(=) &variable_name3.;
                  %else 
                        %let group_by_option=;
                  %let sample_text=;

                  %if %index(&Graphic_type.,Sampling)>0 %then

                                    %do;
                                          proc surveyselect data = &ttt_name.  noprint method = SRS rep = 1 
                                                  sampsize = &N_sample. seed = 12345 out = _tmp_sample_;
                                                  id _all_;
                                          run;

                                          %let ttt_name=_tmp_sample_;
                                          %let sample_text=(sampling%str(=)&N_sample.);
                                    %end;
/*
                  proc sgplot data=&ttt_name.;
                        scatter x=&variable_name1. y=&variable_name2. &group_by_option.;
                        %if %index(&Graphic_type.,Trend)>0 %then

                                    %do;
                                          Loess x=&variable_name1. y=&variable_name2.;
                                          reg x=&variable_name1. y=&variable_name2.;
                                    %end;
                  run;
*/

					proc sgrender data=&ttt_name. template=graph.scattertmp;
					  dynamic _AGE_AT_INJURY="&variable_name1." 
							  _TIME_TO_SURGERY="&variable_name2."
							  _AGE_AT_INJURY2="&variable_name1."
							  _TIME_TO_SURGERY2="&variable_name2."
					;
					run;

                  ods select PearsonCorr;
                  ods output PearsonCorr=_Pearsontmp_;

                  proc corr data=&ttt_name.;
                        var &variable_name1. &variable_name2.;
                  run;

                  ods output close;
                  ods select all;

                  %if %length(&variable_name1.)=32 %then
                              %let p_var1=P%substr(&variable_name1.,1,31);
                  %else
                              %let p_var1=P&variable_name1.;

                  %if %length(&variable_name2.)=32 %then
                              %let p_var2=P%substr(&variable_name2.,1,31);
                  %else
                              %let p_var2=P&variable_name2.;

                  data _Pearsontmp_2;
                        format Task_id $50.;
                        format Table $60.;
                        format Use_Sample $10.;
                        format Variable1 $32.;
                        format Variable2 $32.;
                        format Pearson_Correlation 8.5;
                        format P_value PVALUE8.4;
                        set _Pearsontmp_;
                        %if %index(&Graphic_type.,Sampling)>0 %then
                              %do;
                                    Use_Sample="Yes";
                              %end;
                        %else
                              %do;
                                    Use_Sample="No";
                              %end;

                        task_id="&task_No.";
                        table="&out_ltname.";
                        variable1="&variable_name1.";
                        variable2="&variable_name2.";
                        label task_id="Task ID";
                        label Use_Sample="Use Sample";
                        label Pearson_Correlation="Pearson Correlation Coefficient";
                        label P_Value="P Value";

                        if _n_=1;

                        if &variable_name1.^=1 then
                              do;
                              Pearson_Correlation=&variable_name1.;
                              P_value=&P_var1.;
                              end;
                        else
                              do;
                              Pearson_Correlation=&variable_name2.;
                              P_value=&P_var2.;
                              end;

                        keep Task_id table use_sample Variable1 Variable2 Pearson_Correlation P_value; 
                  run;

                  %if %sysfunc(exist(_pearson_)) %then

                        %do;
                              data _pearson_;
                                    set _pearson_ _Pearsontmp_2;
                              run;
                        %end;
                  %else
                        %do;
                              data _pearson_;
                                    set _Pearsontmp_2;
                              run;
                        %end;

                  proc sql noprint; drop table _Pearsontmp_, _Pearsontmp_2; quit;
                  ods pdf text="^S={%unquote(&pdffontformat.)}Bivariate &task_No.: Scatter Plot&sample_text., &variable_name1. VS &variable_name2.";
                  ods pdf text='^S={font_size=48pt}'; /* spacer */

                  %if %index(&Graphic_type.,Sampling)>0 %then
                                   %do; proc sql noprint; drop table _tmp_sample_;quit; %end;
            %end;
%mend;

 

/*generate analysis list for _all_ type*/

%macro Analysis_Robot;

%let pdffontformat=%str(font_size=14pt font_weight=bold just=center);
/*%let ods_style_set=STYLE%str(=)BarrettsBlue;*/
%let ods_style_set=;
/*
footnote1 "Generated by Analysis Robot: Statistical Analysis Automation System (SAAS)";
footnote2 "Designed by Brian Sun, Rick Hansen Institute, 2011";
*/
 
options dtreset;
options dev = actximg;

%let dt_start=%sysfunc(dateTime(),mdyampm25.2);
%let t_start=%sysfunc(Time());

%if %sysfunc(exist(_all_ext_)) %then %do; proc sql noprint; drop table _all_ext_; quit; %end;
%if %sysfunc(exist(_all_var_infor_)) %then %do; proc sql noprint; drop table _all_var_infor_; quit; %end;
%if %sysfunc(exist(_anova_)) %then %do; proc sql noprint; drop table _anova_; quit; %end;
%if %sysfunc(exist(_bivariable_exclude_reason_)) %then %do; proc sql noprint; drop table _bivariable_exclude_reason_; quit; %end;
%if %sysfunc(exist(_bivariate_)) %then %do; proc sql noprint; drop table _bivariate_; quit; %end;
%if %sysfunc(exist(_bivariate_analysis_)) %then %do; proc sql noprint; drop table _bivariate_analysis_; quit; %end;
%if %sysfunc(exist(_bivariate_analysis_in_)) %then %do; proc sql noprint; drop table _bivariate_analysis_in_; quit; %end;
%if %sysfunc(exist(_bivariate_pair_exclude_)) %then %do; proc sql noprint; drop table _bivariate_pair_exclude_; quit; %end;
%if %sysfunc(exist(_chisq_)) %then %do; proc sql noprint; drop table _chisq_; quit; %end;
%if %sysfunc(exist(_exclude_list_)) %then %do; proc sql noprint; drop table _exclude_list_; quit; %end;
%if %sysfunc(exist(_graphic_type_bi_)) %then %do; proc sql noprint; drop table _graphic_type_bi_; quit; %end;
%if %sysfunc(exist(_graphic_type_uni_)) %then %do; proc sql noprint; drop table _graphic_type_uni_; quit; %end;
%if %sysfunc(exist(_pearson_)) %then %do; proc sql noprint; drop table _pearson_; quit; %end;
%if %sysfunc(exist(_pearson_negative_)) %then %do; proc sql noprint; drop table _pearson_negative_; quit; %end;
%if %sysfunc(exist(_pearson_positive_)) %then %do; proc sql noprint; drop table _pearson_positive_; quit; %end;
%if %sysfunc(exist(_univariate_)) %then %do; proc sql noprint; drop table _univariate_; quit; %end;
%if %sysfunc(exist(_univariate_analysis_)) %then %do; proc sql noprint; drop table _univariate_analysis_; quit; %end;
%if %sysfunc(exist(_univariate_analysis_in_)) %then %do; proc sql noprint; drop table _univariate_analysis_in_; quit; %end;
%if %sysfunc(exist(_variable_exclude_reason_)) %then %do; proc sql noprint; drop table _variable_exclude_reason_; quit; %end;
%if %sysfunc(exist(_variable_include_format_)) %then %do; proc sql noprint; drop table _variable_include_format_; quit; %end;
%if %sysfunc(exist(_variable_list_)) %then %do; proc sql noprint; drop table _variable_list_; quit; %end;
%if %sysfunc(exist(_variable_list_dist_)) %then %do; proc sql noprint; drop table _variable_list_dist_; quit; %end;
%if %sysfunc(exist(_var_exclude_)) %then %do; proc sql noprint; drop table _var_exclude_; quit; %end;
%if %sysfunc(exist(_var_include_)) %then %do; proc sql noprint; drop table _var_include_; quit; %end;
%if %sysfunc(exist(_var_infor_)) %then %do; proc sql noprint; drop table _var_infor_; quit; %end;
%if %sysfunc(exist(_var_infor_dist)) %then %do; proc sql noprint; drop table _var_infor_dist; quit; %end;
%if %sysfunc(exist(_bi_runtime_)) %then %do; proc sql noprint; drop table _bi_runtime_; quit; %end;
%if %sysfunc(exist(_executive_summary_)) %then %do; proc sql noprint; drop table _executive_summary_; quit; %end;
%if %sysfunc(exist(_uni_runtime_)) %then %do; proc sql noprint; drop table _uni_runtime_; quit; %end;

/*

PROC IMPORT OUT= _Univariate_
                         (rename=(SAS_Library=Libname  variable_name=VariableName Table_Name=TableName))
  DATAFILE = "&GRule_full_path_name."
            DBMS=EXCEL REPLACE;
        SHEET="Univariate";
       GETNAMES=YES;
        MIXED=Yes;
      SCANTEXT=YES;
      USEDATE=YES;
      SCANTIME=YES;
 RUN;

PROC IMPORT OUT= _Bivariate_                   
                         (rename=(SAS_Library=Libname  Table_Name=TableName Obs_Marker__optional_=Variable_3))
  DATAFILE = "&GRule_full_path_name."
            DBMS=EXCEL REPLACE;
        SHEET="Bivariate";
       GETNAMES=YES;
        MIXED=Yes;
      SCANTEXT=YES;
      USEDATE=YES;
      SCANTIME=YES;
 RUN;
*/

data _Univariate_;
      format No 8.;
      format libname $32.;
      format Tablename $32.;
      format variablename $32.;
      set _Uni_An(rename=(SAS_Library=Libname  variable_name=VariableName Table_Name=TableName));
      libname=propcase(libname);
      tablename=propcase(tablename);
      variablename=propcase(variablename);
      if libname="" then libname="Work";
      if Graphics_Table_type in ("","Automatic") then Graphics_Table_type="A";
      if upcase(variablename) in ("","_ALL_") then variablename="_";
      if upcase(tablename) in ("","_ALL_") then tablename="_";
      if No>0;
run;

 

data _univariate_ _uni_lib_;
      set _univariate_;
      if tablename="_" then output _uni_lib_;
      else output _univariate_;
run;

proc sql noprint;
      create table _uni_lib_dist_ as
      select distinct libname from _uni_lib_; 
quit;


%let N_lib_uni=;
data _null_;
      set _uni_lib_dist_;
      call symputx('lname_a_uni'||left(put(_n_,8.)),libname);
      call symputx('N_lib_uni',left(put(_n_,8.)));
run;


%if %length(&N_lib_uni)^=0 and &N_lib_uni.^=0 %then
      %do;
                  %do m=1 %to &N_lib_uni.;
                        proc contents data=&&lname_a_uni&m..._all_ noprint out=_tlist_tmp_(where=(memtype="DATA" and substr(memname,1,1)^="_"));
                        run;

                        proc sql noprint; 
                              create table _tlist_dist_ as
                                    select distinct libname, memname as tablename
                                    from _tlist_tmp_;
                              quit;

                        %if &m.=1 %then 
                              %do;
                              data _lib_tlist_; set _tlist_dist_; run;
                              %end;
                        %else 

                              %do;
                              data _lib_tlist_; set _lib_tlist_ _tlist_dist_; run;
                              %end;

                        proc sql noprint; drop table _tlist_tmp_, _tlist_dist_; quit;
                  %end; 

                  proc sql noprint;
                        create table _uni_lib2_ as
                              select A.no,
                                       propcase(A.libname) as libname,
                                       propcase(B.tablename) as tablename,
                                       A.variablename,
                                       A.Graphics_Table_type
                              from _uni_lib_ A
                              left join _lib_tlist_ B
                              on lowcase(A.libname)=lowcase(B.libname)
                        ;
                  quit;

                  proc sort data=_uni_lib2_;
                        by libname tablename;
                  run;

                  data _univariate_;
                        set _uni_lib2_ _univariate_;
                        libname=propcase(libname);
                        No=_n_;
                  run;

                  proc sql noprint; drop table _uni_lib_dist_, _uni_lib_, _uni_lib2_, _lib_tlist_; quit;
      %end;
 

data _Bivariate_;
      format No 8.;
      format libname $32.;
      format Tablename $32.;
      format variable_1 $32.;
      format variable_2 $32.;
      set _Bi_An(rename=(SAS_Library=Libname  Table_Name=TableName Obs_Marker__optional_=Variable_3));
      libname=propcase(libname);
      Tablename=propcase(Tablename);
      variable_1=propcase(variable_1);
      variable_2=propcase(variable_2);
      variable_3=propcase(variable_3);
      if libname="" then libname="Work";
      if Graphics_Table_type in ("","Automatic") then Graphics_Table_type="A";
      if upcase(tablename) in ("","_ALL_") then tablename="_";
      if upcase(variable_1) in ("","_ALL_") then variable_1="_";
      if upcase(variable_2) in ("","_ALL_") then variable_2="_";
      if No>0;
run;

data _bivariate_ _bi_lib_;
      set _bivariate_;
      if tablename="_" then output _bi_lib_;
      else output _bivariate_;
run;

proc sql noprint;
      create table _bi_lib_dist_ as
      select distinct libname from _bi_lib_; 
quit;

%let N_lib_bi=;

data _null_;
      set _bi_lib_dist_;
      call symputx('lname_a_bi'||left(put(_n_,8.)),libname);
      call symputx('N_lib_bi',left(put(_n_,8.)));
run;

%if %length(&N_lib_bi)^=0 and &N_lib_bi.^=0 %then
      %do;
                  %do p=1 %to &N_lib_bi.;
                        proc contents data=&&lname_a_bi&p..._all_ noprint out=_tlist_tmp_(where=(memtype="DATA" and substr(memname,1,1)^="_"));
                        run;

                        proc sql noprint; 
                              create table _tlist_dist_ as
                                    select distinct libname, memname as tablename
                                    from _tlist_tmp_;
                              quit;

                        %if &p.=1 %then 
                              %do;
                              data _lib_tlist_; set _tlist_dist_; run;
                              %end;
                        %else 

                              %do;
                              data _lib_tlist_; set _lib_tlist_ _tlist_dist_; run;
                              %end;

                        proc sql noprint; drop table _tlist_tmp_, _tlist_dist_; quit;

                  %end; 

                  proc sql noprint;
                        create table _bi_lib2_ as
                              select A.no,
                                       propcase(A.libname) as libname,
                                       propcase(B.tablename) as tablename,
                                       propcase(A.variable_1) as variable_1,
                                       propcase(A.variable_2) as variable_2,
                                       propcase(A.variable_3) as variable_3,
                                       A.Graphics_Table_type

                              from _bi_lib_ A
                              left join _lib_tlist_ B
                              on lowcase(A.libname)=lowcase(B.libname)
                        ;
                  quit;

                  proc sort data=_bi_lib2_;
                        by libname tablename;
                  run;

                  data _bivariate_;
                        set _bi_lib2_ _bivariate_;
                        libname=propcase(libname);
                        tablename=propcase(tablename);
                        No=_n_;
                  run;

                  proc sql noprint; drop table _bi_lib_dist_, _lib_tlist_, _bi_lib_, _bi_lib2_; quit;
      %end;

/*
  PROC IMPORT OUT= _Exclude_List_
                        (rename=(SAS_Library=Libname  variable_name=VariableName Table_Name=TableName))
  DATAFILE = "&GRule_full_path_name."
            DBMS=EXCEL REPLACE;
        SHEET="Exclude List";
       GETNAMES=YES;
        MIXED=Yes;
      SCANTEXT=YES;
      USEDATE=YES;
      SCANTIME=YES;
 RUN;


PROC IMPORT OUT= _Adv_Setting_

                        (where=(No^=.))
  DATAFILE = "&GRule_full_path_name."
            DBMS=EXCEL REPLACE;
        SHEET="Advanced - Setting";
       GETNAMES=YES;
        MIXED=Yes;
      SCANTEXT=YES;
      USEDATE=YES;
      SCANTIME=YES;
 RUN;
*/
 

proc sql noprint;
      select value into:Both_Max_N_Values_4_Char from _adv_setting_ where No=1;
      select value into:Both_ID_ratio from _adv_setting_ where No=2;
      select value into:Both_Max_P_Missing from _adv_setting_ where No=3;
      select value into:Uni_Max_N_value_Group from _adv_setting_ where No=4;
      select value into:Uni_Max_Decimal from _adv_setting_ where No=5;
      select value into:Bi_Max_freq_cell from _adv_setting_ where No=6;
      select value into:Bi_Max_Group_N from _adv_setting_ where No=7;
      select value into:Bi_Max_Decimal from _adv_setting_ where No=8;
      select value into:Bi_Max_N_Scatter from _adv_setting_ where No=9;
      select value into:Bi_Min_N_Scatter from _adv_setting_ where No=10;
      select value into:Bi_Sample_N_Scatter from _adv_setting_ where No=11;
      select value into:Bi_Max_N_Trend_Scatter from _adv_setting_ where No=12;
      select value into:switch_off_TS from _adv_setting_ where No=13;
      /*drop table _adv_setting_;*/
quit;

data _Exclude_List_;
      set _Ex_list(rename=(SAS_Library=Libname  variable_name=VariableName Table_Name=TableName));
      format exclude_type_ $10.;
      label exclude_type_="User exclude";
      if lowcase(substr(exclude_type,1,1)) in ("","d") then exclude_type_="Both";
      if lowcase(substr(exclude_type,1,1)) = "u" then exclude_type_="Univariate";
      if lowcase(substr(exclude_type,1,1)) = "b" then exclude_type_="Bivariate";
      if No>0;
      drop exclude_type;
      rename exclude_type_=exclude_type;
run;

 

proc sql noprint;
      create table _variable_list_(rename=(variablename=Var_match)) as
      select distinct propcase(libname) as libname, propcase(tablename) as tablename, propcase(variablename) as variablename
      from _univariate_
      union
      select distinct propcase(libname) as libname, propcase(tablename) as tablename, propcase(variable_1) as variablename
      from _bivariate_
      union
      select distinct propcase(libname) as libname, propcase(tablename) as tablename, propcase(variable_2) as variablename
      from _bivariate_
      order by lowcase(libname),lowcase(tablename), lowcase(variablename)
      ;
quit;

data _variable_list_a_ _variable_list_b_;
      set _variable_list_;
      if Var_match="_" then output _variable_list_a_;
      else output _variable_list_b_;
run;

data _variable_list_b_;
      set _variable_list_b_;
      format VariableName $32.;
      VariableName=Var_match;
run;


%let N_all_=;
data _null_;
      set _variable_list_a_;
      call symputx('lname_a'||left(put(_n_,8.)),LibName);
      call symputx('tname_a'||left(put(_n_,8.)),TableName);
      call symputx('N_all_',left(put(_n_,8.)));
run;


%if %length(&N_all_.)^=0 and &N_all_.^=0 %then
      %do;
                  %do k=1 %to &N_all_.;
                        proc contents data=&&lname_a&k...&&tname_a&k  noprint DETAILS
                                    out=_tmp_(keep=libname memname name  
                                                  rename=(memname=TableName name=VariableName));
                        run;
                        %if &k.=1 %then 
                              %do;
                              data _all_ext_;   set _tmp_;  run;
                              %end;
                        %else 
                              %do;
                              data _all_ext_;   set _all_ext_ _tmp_;    run;
                              %end;

                        proc sql noprint; drop table _tmp_; quit;
                  %end;

                  proc sql noprint;
                        create table _variable_list_a2_ as
                        select A.*,
                        B.VariableName
                        from _variable_list_a_ A
                        left join _all_ext_ B
                        on lowcase(A.libname)=lowcase(B.libname) and
                           lowcase(A.tablename)=lowcase(B.tablename)
                        ;
                  quit;

                  data _variable_list_a_;
                        set _variable_list_a2_;
                  run;

                  proc sql noprint; drop table _variable_list_a2_; quit;

      %end;

data _variable_list_;

      set _variable_list_a_ _variable_list_b_;

run;

proc sql noprint; drop table _variable_list_a_, _variable_list_b_;quit;

proc sql noprint;
      create table _variable_list_dist_ as
            select distinct Propcase(libname) as libname, 
                                    propcase(tablename) as tablename, 
                                    propcase(variablename) as variablename
      from _variable_list_
      ;
quit;

%let N_var=;
%let fid=%sysfunc(open(work._variable_list_dist_));
%let N_var=%sysfunc(attrn(&fid,NOBS));
%let fid=%sysfunc(close(&fid));


%if %length(&N_var.)^=0 and &N_var.^=0 %then
      %do;
                  %PowerContents(input=_variable_list_dist_,output=_Var_infor_dist, mode=list);
                  proc sql noprint;
                        create table _var_infor_ as
                              select 
                                    B.no as No_sub_task,
                                    A.*,
                                    B.type as VariableType,
                                    B.length as VariableLength,
                                    B.Format as VariableFormat,
                                    B.Label as VariableLabel,
                                    B.Nobs as N_obs,
                                    B.N_missing,
                                    B.N_Non_missing,
                                    B.N_dist_value,
                                    B.Nmax,
                                    B.Nmin,
                                    B.integer_dummy
                        from _variable_list_ A
                        left join _Var_infor_dist B
                        on lowcase(A.libname)=lowcase(B.libname) and
                           lowcase(A.tablename)=lowcase(B.tablename) and
                           lowcase(A.variablename)=lowcase(B.variablename)
                        ;
                  quit;

                  proc sort data=_var_infor_;
                        by libname tablename var_match no_sub_task;
                  run;

                  data _var_infor_;
                        set _var_infor_;
                        format Percent_missing percent8.1;
                        format Exclude_for $10.;
                        format Exclude_by $10.;
                        format Exclude_reason $100.;
                        label N_missing="N of Missing Observations";
                        label N_Non_missing="N of Non-Missing Observations";
                        label N_dist_value="N of distinct Non-Missing values";
                        label Percent_missing="% of Missing observations";
                        label Exclude_for="Exclude for which analysis";
                        label Exclude_by="Exclude by";
                        label Exclude_Reason="Exclude Reason";
                        Percent_missing=N_missing/N_obs;
                        if var_match^="_" then no_sub_task=.;
                        if upcase(substr(variableformat,1,8))^="DATETIME" and 
                          upcase(substr(variableformat,1,4)) in ("DATE","YYMM","MMDD","DDMM") 
                        then variableformat="Date";
                        if upcase(substr(variableformat,1,4)) in ("TIME") then variableformat="Time";
                        if upcase(substr(variableformat,1,4))="BEST" then variableformat="";
                        if upcase(substr(variableformat,1,1)) in ("3","4","5","6","7","8") then variableformat="";
                        if upcase(substr(variableformat,1,8))="DATETIME" then variableformat="DateTime";
                        if variabletype=1 and variableformat="" then variableformat="Num";
                        if variabletype=2 then variableformat="Char";
                        AutoExclude=0;
                        if variableformat="Char" and N_dist_value>=&Both_Max_N_Values_4_Char.  then 
                              do;
                                    AutoExclude=1;
                                    Exclude_reason="Exceed the maximum number of distinct value for categorical analysis";
                              end;

                        if N_dist_value<=1 then 
                              do; 
                                    AutoExclude=1;
                                    if N_dist_value=1 then Exclude_reason="Only one non-missing distinct value";
                                    if N_dist_value=0 then Exclude_reason="Missing Values in all records";
                              end;
                        If (integer_dummy=1 and N_dist_value*N_dist_value/N_Non_missing>=&both_ID_ratio. and 
												N_dist_value/N_Non_missing>=0.35)
							or (variableformat="Char" and N_dist_value=N_Non_missing and VariableLength<=50)
						then
                              do;
                                    AutoExclude=1;
                                    Exclude_reason="Identified as ID variable";
                              end;
                        if Percent_missing>&Both_Max_P_Missing. then
                              do;
                                    AutoExclude=1;
                                    %let Percent_M=%sysevalf(&Both_Max_P_Missing.*100);
                                    Exclude_reason="Missing Rate Greater Than &Percent_M.%";
                              end;
                        if AutoExclude=1 and var_match="_" then 
                              do;
                                    Exclude_by="Automatic";
                                    Exclude_for="Both";
                              end;
                        else
                              do;
                                    Exclude_by="";
                                    Exclude_for="";
                                    Exclude_reason="";
                              end;
                  run;

                  proc sql noprint;
                  create table _all_var_infor_ as
                        select A.*,
                                 B.exclude_type
                        from _var_infor_ A
                        left join _exclude_list_ B
                        on lowcase(A.libname)=lowcase(B.libname) and
                           lowcase(A.tablename)=lowcase(B.tablename) and
                           lowcase(A.variablename)=lowcase(B.variablename)
                        order by libname,tablename,var_match,No_sub_task
                  ;
                  quit;

                  data _all_var_infor_;
                        set _all_var_infor_;
                        if exclude_type^="" then
                              do;
                                          Exclude_by="User";
                                          Exclude_for=exclude_type;
                                          Exclude_reason="User";
                              end;
                        drop exclude_type AutoExclude;
                  run;

                  /*proc sql noprint; drop table _var_infor_, _var_infor_dist,_variable_list_dist_,_variable_list_; quit;*/

                  proc sql noprint;
                        create table _var_include_ as
                        select distinct 
                              propcase(libname) as libname,
                              propcase(Tablename) as Tablename,
                              propcase(VariableName) as VariableName label="Included Variable Name",
                              VariableFormat,
                              N_obs,
                              N_missing,
                              Percent_missing,
                              N_Non_missing,
                              N_dist_value

                        from _all_var_infor_(where=(exclude_for^="Both"))
                        order by libname,Tablename,VariableName
                        ;
                  quit;

                  data _var_include_;
                        set _var_include_;
                        if lowcase(libname)^="work" then tablename=cats(libname,".",tablename);
                        drop libname;
                  run;

                  proc sql noprint;
                        create table _var_exclude_ as
                        select distinct 
                              propcase(libname) as libname,
                              propcase(Tablename) as Tablename,
                              propcase(VariableName) as VariableName label="Excluded Variable Name",
                              VariableFormat,
                              N_obs,
                              N_missing,
                              Percent_missing,
                              N_Non_missing,
                              N_dist_value,
                              exclude_for,
                              exclude_by,
                              exclude_Reason

                        from _all_var_infor_(where=(exclude_for^=""))
                        order by libname,Tablename,VariableName
                        ;
                  quit; 

                  data _var_exclude_;
                        set _var_exclude_;
                        if lowcase(libname)^="work" then tablename=cats(libname,".",tablename);
                        drop libname N_missing;
                  run;

   /*generate list for univariate analysis*/

                  proc sql noprint;
                        create table _univariate_analysis_ as
                              select 
                                    A.No as No_task,
                                    B.*,
                                    A.graphics_table_type
                              from _univariate_ A
                              left join _all_var_infor_ B
                              on lowcase(A.libname)=lowcase(B.libname) and
                                 lowcase(A.tablename)=lowcase(B.tablename) and
                                 lowcase(A.variablename)=lowcase(B.var_match)
                              order by No_task, No_sub_task
                              ;
                  quit;

                  data _univariate_analysis_;
                        set _univariate_analysis_;
                        format Auto_Graph_Type $500.;
                        format Analysis_Graphic_type $500.;
                        label graphics_table_type="User-set Graphics/Table Type";
                        label Auto_Graph_Type="Auto-set Graphics/Table Type";
                        label Analysis_Graphic_type="Type of Analysis";
                        if exclude_for in ("","Bivariate");
                        /*drop exclude_for exclude_by;*/
                        if variableformat="Num" then
                              if N_dist_value>&Uni_Max_N_value_Group. then
                                          Auto_Graph_Type="Summary Statistics - Numerical Variable|Histo-Box Plot";
                              else 
                                          Auto_Graph_Type="Summary Statistics - Numerical Variable|Frequecy Table|Horizontal Bar";
                        if variableformat="Char" then
                              if N_dist_value<&Both_Max_N_Values_4_Char. then
                                          Auto_Graph_Type="Frequecy Table|Horizontal Bar";

                        if variableformat="Char" then

                              if N_dist_value<&Both_Max_N_Values_4_Char. then

                                          Auto_Graph_Type="Frequecy Table|Horizontal Bar";

 

                        if variableformat in ("Date","Time","DateTime") then

                                          Auto_Graph_Type="Summary Statistics - Numerical Variable";                          

 

                        if Graphics_Table_type^="A" then  Analysis_Graphic_type=Graphics_Table_type;

                        else  Analysis_Graphic_type=Auto_Graph_Type;

                  run;

 

                  proc sort data=_univariate_analysis_; by no_task no_sub_task; run; quit;

 

                  data _univariate_analysis_;

                        format task_id $20.;

                        label task_id="Task ID";

                        set _univariate_analysis_;

                        by no_task;

                        retain m_tmp (.);

                        if first.no_task and var_match="_" then m_tmp=1;

                        no_sub_task=m_tmp;

                        if last.no_task then m_tmp=.;

                        m_tmp=m_tmp+1;

                        if no_sub_task^=. then task_id=cats(no_task,"-",no_sub_task);

                        else task_id=left(no_task);

                        drop m_tmp;

                  run;

 

                  data _univariate_analysis_in_;

                        set _univariate_analysis_;

                        format type $100.;

                        i=1;

                        do until (scan(analysis_graphic_type,i,"|")="");

                              Type=scan(analysis_graphic_type,i,"|");

                              i=i+1;

                              output;

                        end;

                        keep task_id libname Tablename VariableName Variableformat Type;

                  run;

 

   /*generate list for Bivariate analysis*/

            proc sql noprint;
                  create table _bi_analysis_1 as
                  select 
                        A.No as No_task,
                        B.No_sub_task as No_sub_task1,
                        A.libname,
                        A.Tablename,
                        A.variable_1,
                        B.VariableName as VariableName1,
                        A.variable_2,
                        A.Variable_3,
                        B.N_obs,
                        B.VariableType as VariableType1,
                        B.VariableLength as VariableLength1,
                        B.VariableFormat as VariableFormat1,
                        B.VariableLabel as VariableLabel1,
                        B.N_missing as N_missing1,
                        B.N_Non_missing as N_Non_missing1,
                        B.N_dist_value as N_dist_value1,
                        B.Percent_missing as Percent_missing1,
                        B.Exclude_for as Exclude_for1,
                        B.Exclude_by as Exclude_by1,
                        A.Graphics_Table_type
                  from _bivariate_ A
                  left join _all_var_infor_ B
                  on lowcase(A.libname)=lowcase(B.libname) and
                     lowcase(A.tablename)=lowcase(B.tablename) and
                     lowcase(A.variable_1)=lowcase(B.var_match)
                  order by No_task, No_sub_task1
            ;
            quit;

            proc sql noprint;
                  create table _bivariate_analysis_ as
                  select
                        A.No_task,
                        A.No_sub_task1,
                        B.No_sub_task as No_sub_task2,
                        propcase(A.libname) as libname,
                        propcase(A.Tablename) as Tablename,
                        propcase(A.variable_1) as variable_1,
                        propcase(A.variable_2) as variable_2,
                        propcase(A.VariableName1) as VariableName1,
                        propcase(B.VariableName) as VariableName2,
                        propcase(A.Variable_3) as VariableName3,
                        A.N_obs,
                        A.VariableType1,
                        B.VariableType as VariableType2,
                        A.VariableLength1,
                        B.VariableLength as VariableLength2,
                        A.VariableFormat1,
                        B.VariableFormat as VariableFormat2,
                        A.VariableLabel1,
                        B.VariableLabel as VariableLabel2,
                        A.N_missing1,
                        A.N_Non_missing1,
                        A.N_dist_value1,
                        A.Percent_missing1,
                        B.N_missing as N_missing2,
                        B.N_Non_missing as N_Non_missing2,
                        B.N_dist_value as N_dist_value2,
                        B.Percent_missing as Percent_missing2,
                        A.Exclude_for1,
                        B.Exclude_for as Exclude_for2,
                        A.Exclude_by1,
                        B.Exclude_by as Exclude_by2,
                        A.Graphics_Table_type
                  from _bi_analysis_1 A
                  left join _all_var_infor_ B
                  on lowcase(A.libname)=lowcase(B.libname) and
                     lowcase(A.tablename)=lowcase(B.tablename) and
                     lowcase(A.variable_2)=lowcase(B.var_match)
                  order by No_task, No_sub_task1, No_sub_task2
            ;
            quit;

            proc sql noprint; drop table _bi_analysis_1;quit;

            /*append variable exclude list with datetime variable in automatic bivariate analysis*/

            data auto_drop_datetime_var;
                  set _bivariate_analysis_;
                  if (variable_1="_" and variableformat2 not in ("Num","Char")) or 
                                    (variable_2="_" and variableformat1 not in ("Num","Char")) ;
            run;

            proc sql noprint;
                  create table auto_dist_drop_datetime as
                  select variablename1 as varname from auto_drop_datetime_var
                        where variableformat1 not in ("Num","Char")
                  union 
                  select variablename2 as varname from auto_drop_datetime_var
                        where variableformat2 not in ("Num","Char")
                  ;
            quit;

            proc sql noprint;
                  create table auto_dist_drop_infor as
                  select * from _all_var_infor_
                  where variablename in 
                        (select varname from auto_dist_drop_datetime)
                        and exclude_for not in ("Both","Bivariate");
            quit;

 

            data auto_dist_drop_infor;
                  set auto_dist_drop_infor;
                  format exclude_reason $100.;
                  exclude_for="Bivariate";
                  exclude_by="Automatic";
                  exclude_reason="date/time variables are excluded for bivariate analysis";
                  if lowcase(libname) not in ("work","") then tablename=cats(libname,".",tablename);
                  keep Tablename VariableName VariableFormat N_obs 
                        Percent_missing N_Non_missing N_dist_value 
                        Exclude_for Exclude_by Exclude_reason;
            run;

 

            data _var_exclude_;
                  set _var_exclude_ auto_dist_drop_infor;
            run;

 

            proc sql noprint; 
                  drop table auto_dist_drop_infor,auto_dist_drop_datetime,Auto_drop_datetime_var;
            quit;

 

            /*screen the task list for bivariate analysis and decide the automatic graphic type*/

 

            data _bivariate_analysis_ _bivariate_pair_exclude_;
                        set _bivariate_analysis_;
                        format Auto_Graph_Type $500.;
                        format Analysis_Graphic_type $500.;
                        label graphics_table_type="User-set Graphics/Table Type";
                        label Auto_Graph_Type="Auto-set Graphics/Table Type";
                        label Analysis_Graphic_type="Type of Analysis";
                        if exclude_for1 in ("","Univariate");
                        if exclude_for2 in ("","Univariate");

                        if lowcase(variablename1)^=lowcase(variablename2);

                        if not (variable_1="_" and variable_2="_" and variablename1>variablename2);
                        if not (
                                   (variable_1="_" and variableformat2 not in ("Num","Char")) or 
                                   (variable_2="_" and variableformat1 not in ("Num","Char"))                                
                                    );

                        label variablename1="Variable 1";
                        label variablename2="Variable 2";
                        label variablename3="Marker/ID Variable";

                        /*switch var1 and var2 under condition*/
                        format VariableName $32.;
                        format VariableType 8.;
                        format VariableLength 8.;
                        format VariableFormat $32.;
                        format VariableLabel $256.;
                        format N_missing 8.;
                        format N_Non_missing 8.;
                        format N_dist_value 8.;
                        format Percent_missing percent8.1;
                        format Exclude_for $10.;
                        format Exclude_by $10.;

                        if (VariableFormat1="Char" and VariableFormat2="Num") or
                           (VariableFormat1="Num" and VariableFormat2="Num" 
                              and N_dist_value2>&Bi_Max_Group_N.
                              and N_dist_value1<=&Bi_Max_Group_N.
							  and substr(graphics_table_type,1,1)='A') 
							  
							then
                              do;
                                    VariableName=VariableName1;
                                    VariableType=VariableType1;
                                    VariableLength=VariableLength1;
                                    VariableFormat=VariableFormat1;
                                    VariableLabel=VariableLabel1;
                                    N_missing=N_missing1;
                                    N_Non_missing=N_Non_missing1;
                                    N_dist_value=N_dist_value1;
                                    Percent_missing=Percent_missing1;
                                    Exclude_for=Exclude_for1;
                                    Exclude_by=Exclude_by1;

                                    VariableName1=VariableName2;
                                    VariableType1=VariableType2;
                                    VariableLength1=VariableLength2;
                                    VariableFormat1=VariableFormat2;
                                    VariableLabel1=VariableLabel2;
                                    N_missing1=N_missing2;
                                    N_Non_missing1=N_Non_missing2;
                                    N_dist_value1=N_dist_value2;
                                    Percent_missing1=Percent_missing2;
                                    Exclude_for1=Exclude_for2;
                                    Exclude_by1=Exclude_by2;

                                    VariableName2=VariableName;
                                    VariableType2=VariableType;
                                    VariableLength2=VariableLength;
                                    VariableFormat2=VariableFormat;
                                    VariableLabel2=VariableLabel;
                                    N_missing2=N_missing;
                                    N_Non_missing2=N_Non_missing;
                                    N_dist_value2=N_dist_value;
                                    Percent_missing2=Percent_missing;
                                    Exclude_for2=Exclude_for;
                                    Exclude_by2=Exclude_by;

									switch=1;
                              end;

                              drop VariableName VariableType VariableLength VariableFormat 
                                    VariableLabel N_missing N_Non_missing N_dist_value 
                                    Percent_missing Exclude_for Exclude_by;

                              /*apply rules to assign graphic type*/

                              if find(Graphics_Table_type,"Scatter")>0 and N_obs>&Bi_Max_N_Scatter.
                                    then 
                                          Graphics_Table_type=cats(Graphics_Table_type,"(Sampling)");
                              if N_dist_value1*N_dist_value2<=&Bi_Max_freq_cell.
                              		then Auto_Graph_Type="2*2 Frequency Analysis";

                              if variableformat1="Num" and variableformat2="Num" and N_dist_value1*N_dist_value2>=&Bi_Min_N_Scatter. 
                              then 
                                    do;
                                          if N_obs>&Bi_Max_N_Trend_Scatter. Then 
                                                do;
                                                      if Auto_Graph_Type="" then Auto_Graph_Type="Scatter Plot";
                                                      else Auto_Graph_Type=cats(Auto_Graph_Type,"|Scatter Plot");
                                                end;
                                          else
                                                do;
                                                      if Auto_Graph_Type="" then Auto_Graph_Type="Scatter Plot With Trend Lines";
                                                      else Auto_Graph_Type=cats(Auto_Graph_Type,"|Scatter Plot With Trend Lines");
                                                end;

                                          if N_obs>&Bi_Max_N_Scatter. then Auto_Graph_Type=cats(Auto_Graph_Type,"(Sampling)");
                                    end;

                              if N_dist_value1<=&Bi_Max_Group_N. and N_dist_value2<=&Bi_Max_Group_N.
                              then 
                                    if Auto_Graph_Type="" then Auto_Graph_Type="Horizontal Bar Chart by Group";
                                    else Auto_Graph_Type=cats(Auto_Graph_Type,"|Horizontal Bar Chart by Group");

                              if variableformat1="Num" and (N_dist_value1>&Bi_Max_Group_N. and N_dist_value2<=&Bi_Max_Group_N.)
                              then 
                                    if Auto_Graph_Type="" then Auto_Graph_Type="Summary Statistics By Group";
                                    else Auto_Graph_Type=cats(Auto_Graph_Type,"|Summary Statistics By Group");

                              if variableformat1="Num" and (N_dist_value1>&Bi_Max_Group_N. and N_dist_value2<=&Bi_Max_Group_N.)
                              then 
                                    if Auto_Graph_Type="" then Auto_Graph_Type="Box Plot By Group";
                                    else Auto_Graph_Type=cats(Auto_Graph_Type,"|Box Plot By Group");

                              if variableformat1="Num" and N_dist_value2<=&Bi_Max_Group_N.
                              then 
                                    if Auto_Graph_Type="" then Auto_Graph_Type="Mean Plot Chart by Group";
                                    else Auto_Graph_Type=cats(Auto_Graph_Type,"|Mean Plot Chart by Group");

                        if Graphics_Table_type^="A" then  Analysis_Graphic_type=Graphics_Table_type;
	                        else  Analysis_Graphic_type=Auto_Graph_Type;

                        if Analysis_Graphic_type="" then output _bivariate_pair_exclude_;
	                        else output _bivariate_analysis_;
            run;


                  proc sort data=_bivariate_analysis_; by no_task no_sub_task1 no_sub_task2; run; quit;

                  data _bivariate_analysis_;
                        format task_id $20.;
                        format no_sub_task 8.;
                        label task_id="Task ID";
                        set _bivariate_analysis_;
                        by no_task;
                        retain m_tmp (.);
                        if first.no_task and (variable_1="_" or variable_2="_") then m_tmp=1;
                        no_sub_task=m_tmp;
                        if last.no_task then m_tmp=.;
                        m_tmp=m_tmp+1;
                        if no_sub_task^=. then task_id=cats(no_task,"-",no_sub_task);
                        else task_id=left(no_task);
                        drop m_tmp no_sub_task;
                  run;

 

                  data _bivariate_analysis_in_;
                        set _bivariate_analysis_;
                        format type $100.;
                        i=1;
                        do until (scan(analysis_graphic_type,i,"|")="");
                              Type=scan(analysis_graphic_type,i,"|");
                              i=i+1;
                              output;
                        end;
                        keep task_id libname Tablename 
                              VariableName1 VariableName2 VariableName3 
                              Variableformat1 variableformat2 Type;
                  run;

 

/*drop libname for display*/

 

            data _univariate_analysis_;
                  set _univariate_analysis_;
                  if lowcase(libname) not in ("","work") then tablename=cats(libname,".",tablename);
                  drop libname;
            run;

 

            data _bivariate_analysis_;

                  set _bivariate_analysis_;

                  if lowcase(libname) not in ("","work") then tablename=cats(libname,".",tablename);

                  drop libname;

            run;

 

            

            data _bivariate_pair_exclude_;

                  set _bivariate_pair_exclude_;

                  if lowcase(libname) not in ("","work") then tablename=cats(libname,".",tablename);

                  drop libname;

            run;

 

            data _bivariate_pair_exclude_;

                  set _bivariate_pair_exclude_;

                  Exclude_for1="Bivariate";

                  Exclude_by1="Automatic";

                  format Exclude_reason $100.;

                  

                  label  VariableFormat1="Variable 1 Format";

                  label  VariableFormat2="Variable 2 Format";

                  label  N_dist_value1="Variable 1: N of distinct Non-Missing values";

                  label  N_dist_value2="Variable 2: N of distinct Non-Missing values";

                  label Exclude_reason="Exclude Reason";

                  

                  if lowcase(variableformat1) in ("date","datetime","time") or

                        lowcase(variableformat2) in ("date","datetime","time")

                        then Exclude_reason="Exclude for Date/Time format";

 

                  if lowcase(variableformat1)="char" and

                        lowcase(variableformat2)="char" and N_dist_value1*N_dist_value2>&Bi_Max_freq_cell.

                        then Exclude_reason="Categorical variables:Exceed the max number of cells for 2*2 frequency table";

 

                  if lowcase(variableformat1)="num" and

                        lowcase(variableformat2)="char" and N_dist_value2>&Bi_Max_Group_N.

                        then Exclude_reason="Categorical variable: exceed the max number of groups for analysis";

 

                  keep Tablename VariableName1 VariableName2  

                  N_obs VariableFormat1 VariableFormat2  

                  N_dist_value1 N_dist_value2 

                  Exclude_for1  Exclude_by1 Exclude_reason;

            run;

 

 

            %let N_Uni=;

            data _null_;

                  set _univariate_analysis_in_;

                  call symputx('Uni_Task_No'||left(put(_n_,8.)),task_id);

                  call symputx('Uni_lname'||left(put(_n_,8.)),LibName);

                  call symputx('Uni_tname'||left(put(_n_,8.)),TableName);

                  call symputx('Uni_vname'||left(put(_n_,8.)),VariableName);

                  call symputx('Uni_gtype'||left(put(_n_,8.)),type);

                  call symputx('Uni_vformat'||left(put(_n_,8.)),Variableformat);

                  call symputx('N_Uni',left(put(_n_,8.)));

            run;

 

            %let N_Bi=;

            data _Null_;

                        set _bivariate_analysis_in_;

                  call symputx('Bi_Task_No'||left(put(_n_,8.)),task_id);

                        call symputx('Bi_lname'||left(put(_n_,8.)),LibName);

                        call symputx('Bi_tname'||left(put(_n_,8.)),TableName);

                        call symputx('Bi_vname3'||left(put(_n_,8.)),VariableName3);

                        call symputx('Bi_vname1'||left(put(_n_,8.)),VariableName1);

                        call symputx('Bi_vname2'||left(put(_n_,8.)),VariableName2);

                        call symputx('Bi_gtype'||left(put(_n_,8.)),type);

                        call symputx('N_Bi',left(put(_n_,8.)));

            run;

           /* DM 'clear log';*/

            title;
			/*
			ods html;
			*/
            ods escapechar='^';
            ods noproctitle;
            ods listing close;
            ods graphics on;

            options orientation=portrait;
 /*****************************Univariate Analysis****************************/

                  %if %sysfunc(exist(_Uni_RunTime_)) %then %do; proc sql noprint; drop table _Uni_RunTime_; quit; %end;
                  %if %length(&N_Uni.)^=0 and &N_Uni.^=0 %then

                        %do;

                                    ods pdf &ods_style_set.  startpage=never file="&pdf_out_path_filename2.";

 

                                    ods pdf text='^S={font_size=90pt}'; /* spacer */

                                    ods pdf text="^S={%unquote(&pdffontformat.)}Univariate Analysis";

                                    ods pdf text='^S={font_size=32pt}'; /* spacer */

 

                                    %do i=1 %to &N_Uni.;

                                                      %let step_start=%sysfunc(Time());

 

                                                      %put "Now working on Univariate Analysis (&&Uni_gtype&i): &&Uni_Task_No&i &&Uni_vname&i, %eval(&N_bi+&N_Uni-&i+1) tasks left";

 

                                                      %uni_graphics(task_No=&&Uni_Task_No&i,

                                                                          library_name=&&Uni_lname&i,

                                                                          table_name=&&Uni_tname&i,

                                                                          variable_name=&&Uni_vname&i,

                                                                          variable_format=&&Uni_vformat&i,

                                                                          Graphic_type=&&Uni_gtype&i,

                                                                          N_deci=&Uni_Max_Decimal.);

 

                                                      %let step_end=%sysfunc(Time());

                                                      /*%let Step_run_time=%sysfunc(putn(%sysevalf(&step_end.-&step_start.),time8.));*/

                                                      %let Step_run_time=%sysevalf(&step_end.-&step_start.);

                                                      data _uniruntime_tmp_;

                                                            format task_id $20.;

                                                            format libname $32.;

                                                            format tablename $32.;

                                                            format variablename $32.;

                                                            format variableformat $32.;

                                                            format GraphicType $100.;

                                                            format runtime time8.;

 

                                                            task_id="&&Uni_Task_No&i";

                                                            libname="&&Uni_lname&i";

                                                            tablename="&&Uni_tname&i";

                                                            variablename="&&Uni_vname&i";

                                                            variableformat="&&Uni_vformat&i";

                                                            GraphicType="&&Uni_gtype&i";

                                                            runtime=&Step_run_time.;

 

                                                            label task_id="Task ID";

                                                            label libname="Library Name";

                                                            label tablename="Table Name";

                                                            label variablename="Variable Name";

                                                            label variableformat="Variable Format";

                                                           label GraphicType="Graphics Type";

                                                            label runtime="Run Time";

                                                      run;

                                                      %if %sysfunc(exist(_Uni_RunTime_)) %then 

                                                                  %do; data _Uni_RunTime_; set _Uni_RunTime_ _uniruntime_tmp_;run; %end;

                                                      %else

                                                                  %do; data _Uni_RunTime_; set _uniruntime_tmp_;run; %end;

                                                      proc sql noprint; drop table _uniruntime_tmp_;quit;



                                                      /*%if %sysfunc(mod(&i.,50))=0 %then %do; DM 'clear log';%end;*/

 

                                    %end;

                                    ods pdf close;

                  %end;


 /*****************************Bivariate Analysis****************************/

                  %if %sysfunc(exist(_Bi_RunTime_)) %then %do; proc sql noprint; drop table _Bi_RunTime_; quit; %end;

                  %if %sysfunc(exist(_pearson_)) %then %do; proc sql noprint; drop table _pearson_; quit; %end;

                  %if %sysfunc(exist(_chisq_)) %then %do; proc sql noprint; drop table _chisq_; quit; %end;

                  %if %sysfunc(exist(_anova_)) %then %do; proc sql noprint; drop table _anova_; quit; %end;

                  

                  %if %length(&N_Bi.)^=0 and &N_Bi.^=0 %then

                        %do;

                              ods pdf &ods_style_set.  startpage=yes file="&pdf_out_path_filename3.";

 

                              ods pdf text='^S={font_size=90pt}'; /* spacer */

                             ods pdf text="^S={%unquote(&pdffontformat.)}Bivariate Analysis";

                              ods pdf text='^S={font_size=32pt}'; /* spacer */    

 

                                    %do i=1 %to &N_Bi.;

                                                      %let step_start=%sysfunc(Time());

                                                      %put "Now working on Bivariate Analysis (&&Bi_gtype&i): &&Bi_Task_No&i &&bi_vname1&i VS &&bi_vname2&i, %eval(&N_bi-&i+1) tasks left";

                                                      %bi_graphics(task_No=&&Bi_Task_No&i,

                                                                        lib_name=&&bi_lname&i,

                                                                        table_name=&&bi_tname&i,

                                                                         variable_name1=&&bi_vname1&i,

                                                                         variable_name2=&&bi_vname2&i,

                                                                         variable_name3=,

                                                                         Graphic_type=&&Bi_gtype&i,

                                                                          N_deci=&Bi_Max_Decimal.,

                                                                          N_sample=&Bi_Sample_N_Scatter.);

 

                                                      %let step_end=%sysfunc(Time());

                                                      %let Step_run_time=%sysevalf(&step_end.-&step_start.);

                                                      data _biruntime_tmp_;

                                                            format task_id $20.;

                                                            format libname $32.;

                                                            format tablename $32.;

                                                            format variablename1 $32.;

                                                            format variablename2 $32.;

                                                            format GraphicType $100.;

                                                            format runtime time8.;

 

                                                            task_id="&&bi_Task_No&i";

                                                            libname="&&bi_lname&i";

                                                            tablename="&&bi_tname&i";

                                                            variablename1="&&bi_vname1&i";

                                                            variablename2="&&bi_vname2&i";

                                                            GraphicType="&&bi_gtype&i";

                                                            runtime=&Step_run_time.;

 

                                                            label task_id="Task ID";

                                                            label libname="Library Name";

                                                            label tablename="Table Name";

                                                            label variablename1="Variable 1";

                                                            label variablename2="Variable 2";

                                                           label GraphicType="Graphics Type";

                                                            label runtime="Run Time";

                                                      run;

                                                      %if %sysfunc(exist(_bi_RunTime_)) %then 

                                                                  %do; data _bi_RunTime_; set _bi_RunTime_ _biruntime_tmp_;run; %end;

                                                      %else

                                                                  %do; data _bi_RunTime_; set _biruntime_tmp_;run; %end;

                                                      proc sql noprint; drop table _biruntime_tmp_;quit;

                                                     /* %if %sysfunc(mod(&i.,50))=0 %then %do; DM 'clear log';%end;*/

                                    %end;

 

                                    %if %sysfunc(exist(_pearson_)) %then 

                                          %do;

                                                data _pearson_positive_ _pearson_negative_;

                                                      set _pearson_;

                                                      if P_Value>=0;

                                                      if pearson_correlation>=0 then output _pearson_positive_;

                                                      else output _pearson_negative_;

                                                run;

 

                                                proc sort data=_pearson_positive_;

                                                      by descending Pearson_correlation;

                                                run;

 

                                                proc sort data=_pearson_negative_;

                                                      by Pearson_correlation;

                                                run;

 

                                                /*proc sql noprint; drop table _pearson_; quit;*/

                                          %end;

 

                                    %if %sysfunc(exist(_chisq_)) %then 

                                          %do;

                                                data _chisq_;

                                                      set _chisq_;

                                                      if P_value>=0;

                                                run;

                                                proc sort data=_chisq_;

                                                      by P_value;

                                                run;

                                          %end;

 

                                    %if %sysfunc(exist(_anova_)) %then 

                                          %do;

                                                data _anova_;

                                                      set _anova_;

                                                      if ProbF>=0;

                                                run;

                                                proc sort data=_anova_;

                                                      by ProbF;

                                                run;

                                          %end;

 

                              ods pdf close;

                        %end;

/*Executive report*/

 

                  %let Nf_include=0;

                  %let N_table_include=0;

                  %if %sysfunc(exist(_var_include_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._var_include_));

                              %let Nf_include=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                              proc sql noprint;

                                    select count(distinct tablename) into: N_table_include

                                    from _var_include_

                              quit;

                        %end;

 

                  %let Nf_exclude=0;

                  %if %sysfunc(exist(_var_exclude_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._var_exclude_));

                              %let Nf_exclude=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                        %end;

 

                  %let Nf_pair_exclude=0;

                  %if %sysfunc(exist(_bivariate_pair_exclude_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._bivariate_pair_exclude_));

                              %let Nf_pair_exclude=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                        %end;

 

                  %let Nf_uni=0;

                  %if %sysfunc(exist(_univariate_analysis_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._univariate_analysis_));

                              %let Nf_uni=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                        %end;

 

                  %let Nf_bi=0;

                  %if %sysfunc(exist(_bivariate_analysis_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._bivariate_analysis_));

                              %let Nf_bi=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                        %end;

 

                  %let N_Pearson=0;

                  %if %sysfunc(exist(_pearson_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._pearson_));

                              %let N_Pearson=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                              proc sql noprint;

                                    select count(*) into: N_Sig_Pearson

                                    from _pearson_

                                    where P_Value<=0.05 and P_Value>=0;

                              quit;

                        %end;

 

                  %let Np_pos=0;

                  %if %sysfunc(exist(_pearson_positive_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._pearson_positive_));

                              %let Np_pos=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                        %end;

 

                  %let Np_neg=0;

                  %if %sysfunc(exist(_pearson_negative_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._pearson_negative_));

                              %let Np_neg=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                        %end;

 

                  %let N_chisq=0;

                  %if %sysfunc(exist(_chisq_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._chisq_));

                              %let N_chisq=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                              proc sql noprint;

                                    select count(*) into: N_Sig_ChiSQ

                                    from _chisq_

                                    where P_Value<=0.05 and P_Value>=0;

                              quit;

                        %end;

 

                  %let N_anova=0;

                  %if %sysfunc(exist(_anova_)) %then 

                        %do;

                              %let fid=%sysfunc(open(work._anova_));

                              %let N_anova=%sysfunc(attrn(&fid,NOBS));

                              %let fid=%sysfunc(close(&fid));

                              proc sql noprint;

                                    select count(*) into: N_Sig_ANOVA

                                    from _ANOVA_

                                    where ProbF<=0.05 and ProbF>=0;

                              quit;

                        %end;

            %if %length(&N_Uni.)=0 %then %let N_Uni=0;
            %if %length(&N_bi.)=0 %then %let N_bi=0;
            %if %length(&N_bi.)=0 %then %let N_bi=0;
            %let N_Total=%eval(&N_Bi.+&N_Uni.);
            %let N_Test=%eval(&Np_pos.+&Np_neg.+&N_chisq.+&N_anova.);
            %let Uni_type_N=0;
            %if &N_Uni>0  %then 
                  %do;
                        proc sql noprint;
                        create table _graphic_type_Uni_ as
                                    select type, count(type) as N_type
                                    from _univariate_analysis_in_
                                    group by type
                                    order by type
                              ;
                        quit;

                        data _null_;
                              set _graphic_type_Uni_;
                              call symputx('Uni_type'||left(put(_n_,8.)),type);
                              call symputx('Uni_type_count'||left(put(_n_,8.)),n_type);
                              call symputx('Uni_type_n',left(put(_n_,8.)));
                        run;

                  %end;

            %let bi_type_n=0;

            %if &N_bi>0  %then 

                  %do;

                        proc sql noprint;

                        create table _graphic_type_bi_ as

                                    select type, count(type) as N_type

                                    from _bivariate_analysis_in_

                                    group by type

                                    order by type

                              ;

                        quit;

                        data _null_;

                              set _graphic_type_bi_;

                              call symputx('bi_type'||left(put(_n_,8.)),type);

                              call symputx('bi_type_count'||left(put(_n_,8.)),n_type);

                              call symputx('bi_type_n',left(put(_n_,8.)));

                        run;

                  %end;

            %let N_sumformat=0;

            %if &Nf_include.>0 %then

                  %do;

                        proc sql noprint;

                                    create table _variable_include_format_ as

                                    select VariableFormat,count(*) as N_format

                                    from _var_include_

                                    group by VariableFormat

                                    order by VariableFormat

                                    ;

                        quit;

                        data _null_;

                              set _variable_include_format_;

                              call symputx('sumformat'||left(put(_n_,8.)),variableformat);

                              call symputx('sumformat_count'||left(put(_n_,8.)),n_format);

                              call symputx('N_sumformat',left(put(_n_,8.)));

                        run;

                  %end;


            %let N_ExcludeReason=0;
            %if &Nf_exclude.>0 %then
                  %do;
                        proc sql noprint;
                                    create table _variable_exclude_reason_ as
                                    select Exclude_reason,count(*) as N_reason
                                    from _var_exclude_
                                    group by Exclude_reason
                                    order by Exclude_reason
                                    ;
                        quit;

                        data _null_;
                              set _variable_exclude_reason_;
                             call symputx('ExcludeReason'||left(put(_n_,8.)),Exclude_reason);
                             call symputx('excludereason_count'||left(put(_n_,8.)),n_reason);
                              call symputx('N_ExcludeReason',left(put(_n_,8.)));
                        run;
                  %end;

 

            %let N_PairExcludeReason=0;

            %if &Nf_pair_exclude.>0 %then

                  %do;

                        proc sql noprint;

                                    create table _bivariable_exclude_reason_ as

                                    select Exclude_reason,count(*) as N_reason

                                    from _bivariate_pair_exclude_

                                    group by Exclude_reason

                                    order by Exclude_reason

                                    ;

                        quit;

                        data _null_;

                              set _bivariable_exclude_reason_;

                              call symputx('PairExcludeReason'||left(put(_n_,8.)),Exclude_reason);

                              call symputx('PairExcludeReason_count'||left(put(_n_,8.)),n_reason);

                              call symputx('N_PairExcludeReason',left(put(_n_,8.)));

                        run;

                  %end;

 

 

            %let rep_N=10;

            %let header_Char=.;

 

            data _executive_summary_;

                  format Item $256.;

                  format value 8.;

 

                  Item="Total Number of Tables Included for Analysis";

                  value=&N_table_include.;

                  output;

 

                  Item="Total Number of Variables Included for Analysis";

                  value=&Nf_include.;

                  output;

                  %let u=a;
                  %if &N_sumformat.>0 %then
                        %do i=1 %to &N_sumformat.;
                             Item="&header_Char."||repeat(' ',&rep_n.)||"&u.) &&sumformat&i";
                              value=&&sumformat_count&i;

                              %if &u=f %then %let u=g;
                              %if &u=d %then %let u=f;
                              %if &u=c %then %let u=d;
                              %if &u=b %then %let u=c;
                              %if &u=a %then %let u=b;
                              output;
                        %end;

                  Item="Total Number of Variables Excluded for Analysis";
                  value=&Nf_exclude.;
                  output;

                  %let u=a;
                  %if &N_ExcludeReason.>0 %then
                        %do i=1 %to &N_ExcludeReason.;
                              Item="&header_Char."||repeat(' ',&rep_n.)||"&u.) &&ExcludeReason&i";
                              value=&&ExcludeReason_count&i;
                              %if &u=f %then %let u=g;
                              %if &u=d %then %let u=f;
                              %if &u=c %then %let u=d;
                              %if &u=b %then %let u=c;
                              %if &u=a %then %let u=b;
                              output;
                        %end;

                  Item="Total Number of Variable Pairs Excluded for Bivariate Analysis";
                  value=&Nf_pair_exclude.;
                  output;
                  %let u=a;
                  %if &N_PairExcludeReason.>0 %then
                        %do i=1 %to &N_PairExcludeReason.;
                              Item="&header_Char."||repeat(' ',&rep_n.)||"&u.) &&PairExcludeReason&i";
                              value=&&PairExcludeReason_count&i;
                              %if &u=f %then %let u=g;
                              %if &u=d %then %let u=f;
                              %if &u=c %then %let u=d;
                              %if &u=b %then %let u=c;
                              %if &u=a %then %let u=b;
                              output;
                        %end;

                  Item="Total Number of Statistical Tables / Graphics Generated";
                  value=&N_Total.;
                  output;
                  Item="&header_Char."||repeat(' ',&rep_n.)||"a) For Univariate Analysis";
                  value=&N_Uni.;
                  output;
                  %if &Uni_type_n.>0 %then
                        %do i=1 %to &Uni_type_n.;
                              Item="&header_Char."||repeat(' ',&rep_n.*2)||"&i.) &&Uni_type&i";
                              value=&&Uni_type_count&i;
                              output;
                        %end;
                  Item="&header_Char."||repeat(' ',&rep_n.)||"b) For Bivariate Analysis";
                  value=&N_Bi.;
                  output;
                  %if &bi_type_n.>0 %then

                        %do i=1 %to &bi_type_n.;

                             Item="&header_Char."||repeat(' ',&rep_n.*2)||"&i.) &&bi_type&i";

                              value=&&bi_type_count&i;

                              output;

                        %end;
                  Item="Total Number of Statistical Tests";
                  value=&N_Test.;
                  output;
                  %let u=a;
                  %if &N_Pearson.>0 %then
                        %do;
                              Item="&header_Char."||repeat(' ',&rep_n.)||"a) Pearson Correlation Test";
                              value=&N_Pearson.;
                              output;
                              Item="&header_Char."||repeat(' ',&rep_n.*2)||"1) Number of Significant Tests (P Value<=0.05)";
                              value=&N_Sig_Pearson.;
                              %let u=b;
                              output;
                        %end;

                  %if &N_chisq.>0 %then
                        %do;
                              Item="&header_Char."||repeat(' ',&rep_n.)||"&u.) Chi-Square Test";
                              value=&N_chisq.;
                              output;
                              Item="&header_Char."||repeat(' ',&rep_n.*2)||"1) Number of Significant Tests (P Value<=0.05)";
                              value=&N_Sig_ChiSQ.;
                              %if &u.=a %then %let u=b;
                              %else %let u=c;
                              output;
                        %end;
                  %if &N_ANOVA.>0 %then
                        %do;
                              Item="&header_Char."||repeat(' ',&rep_n.)||"&u.) ANOVA Analysis";
                              value=&N_ANOVA.;
                              output;
                              Item="&header_Char."||repeat(' ',&rep_n.*2)||"1) Number of Significant Tests (P Value<=0.05)";
                              value=&N_Sig_ANOVA.;
                              output;
                        %end;
            run;

            options orientation=landscape;
            ods pdf &ods_style_set.  startpage=yes file="&pdf_out_path_filename1.";

            title1 "Executive Summary";
            proc print data=_executive_summary_ label; run;

            %let dt_end=%sysfunc(dateTime(),mdyampm25.2);
            %let t_end=%sysfunc(Time());
            %let run_time=%sysfunc(putn(%sysevalf(&t_end.-&t_start.),time8.));


            ods pdf text="^S={font_size=12pt font_weight=bold just=center}Total run time: &run_time.";
            ods pdf text='^S={font_size=30pt}'; /* spacer */
            ods pdf text="^S={font_size=18pt font_weight=bold just=center}Contents";
            ods pdf text='^S={font_size=20pt}'; /* spacer */
            %let titlenum=1;

/*Contents: summary of variables included*/
            %let listfont=14;
            %let header_Blank=%str(                                        );
            %let breakerfont=10;
                              %if &Nf_include.^=0 %then 

                              %do;
                                    ods pdf text="^S={font_size=&listfont.pt font_weight=bold just=left}&header_blank.0-&titlenum.: List of Variables Included for Analysis";
                                    %let titlenum=%eval(&titlenum+1);
                                    ods pdf text='^S={font_size=&breakerfont.pt}'; /* spacer */
                              %end;
/*Contents: summary of variables excluded*/

                              %if &Nf_exclude.^=0 %then 
                              %do;
                                    ods pdf text="^S={font_size=&listfont.pt font_weight=bold just=left}&header_blank.0-&titlenum.: List of Variables Excluded for Analysis";
                                    %let titlenum=%eval(&titlenum+1);
                                    ods pdf text='^S={font_size=&breakerfont.pt}'; /* spacer */
                              %end;
/*Contents: summary of variable pairs excluded*/
                              %if &Nf_pair_exclude.^=0 %then 
                              %do;
                                    ods pdf text="^S={font_size=&listfont.pt font_weight=bold just=left}&header_blank.0-&titlenum.: List of Variable Pairs Excluded for Bivariate Analysis";
                                    %let titlenum=%eval(&titlenum+1);
                                    ods pdf text='^S={font_size=&breakerfont.pt}'; /* spacer */
                              %end;

/*Contents: summary of univariate analysis*/
                              %if &Nf_uni.^=0 %then 

                              %do;
                                    ods pdf text="^S={font_size=&listfont.pt font_weight=bold just=left}&header_blank.0-&titlenum.: List of Statistical Graphics and Tables For Univariate Analysis";
                                    %let titlenum=%eval(&titlenum+1);
                                    ods pdf text='^S={font_size=&breakerfont.pt}'; /* spacer */
                              %end;

 

/*Contents: summary of bivariate analysis*/

                              %if &Nf_bi.^=0 %then 

                              %do;
                                    ods pdf text="^S={font_size=&listfont.pt font_weight=bold just=left}&header_blank.0-&titlenum.: List of Statistical Graphics and Tables For Bivariate Analysis";
                                    %let titlenum=%eval(&titlenum+1);
                                    ods pdf text='^S={font_size=&breakerfont.pt}'; /* spacer */
                              %end;
/*Contents: positive pearson table*/

 

                              %if &Np_pos.^=0 %then 

                              %do;

                                    ods pdf text="^S={font_size=&listfont.pt font_weight=bold just=left}&header_blank.0-&titlenum.: Summary of Pearson Correlation Test(Positive) Results";

                                    %let titlenum=%eval(&titlenum+1);

                                    ods pdf text='^S={font_size=&breakerfont.pt}'; /* spacer */

                              %end;
/*Contents: negative pearson table*/
                              %if &Np_neg.^=0 %then 
                              %do;
                                    ods pdf text="^S={font_size=&listfont.pt font_weight=bold just=left}&header_blank.0-&titlenum.: Summary of Pearson Correlation Test(negative) Results";
                                    %let titlenum=%eval(&titlenum+1);
                                    ods pdf text='^S={font_size=&breakerfont.pt}'; /* spacer */
                              %end;

/*Contents: Chi-SQ test table*/
                              %if &N_chisq.^=0 %then 
                              %do;
                                    ods pdf text="^S={font_size=&listfont.pt font_weight=bold just=left}&header_blank.0-&titlenum.: Summary of Chi-Square Test Results";
                                    %let titlenum=%eval(&titlenum+1);
                                    ods pdf text='^S={font_size=&breakerfont.pt}'; /* spacer */
                              %end;
/*Contents: ANOVA test table*/
                              %if &N_anova.^=0 %then 
                              %do;
                                    ods pdf text="^S={font_size=&listfont.pt font_weight=bold just=left}&header_blank.0-&titlenum.: Summary of ANOVA Analysis Results";
                                    %let titlenum=%eval(&titlenum+1);
                              %end;

            %let titlenum=1;

/*print summary of variables included*/
                              %if &Nf_include.^=0 %then 
                              %do;
                                    title1 "0-&titlenum.: List of Variables Included for Analysis";
                                    proc print data=_var_include_ label; run;
                                    %let titlenum=%eval(&titlenum+1);
                              %end;

/*print summary of variables excluded*/

                              %if &Nf_exclude.^=0 %then 
                              %do;
                                    title1 "0-&titlenum.: List of Variables Excluded for Analysis";
                                    proc print data=_var_exclude_(drop=N_Non_missing) label;run;
                                    %let titlenum=%eval(&titlenum+1);
                              %end;

/*print summary of variable pairs excluded*/

                              %if &Nf_pair_exclude.^=0 %then 
                              %do;
                                    title1 "0-&titlenum.: List of Variable Pairs Excluded for Bivariate Analysis";
                                    proc print data=_bivariate_pair_exclude_(drop=N_obs) label;run;
                                    %let titlenum=%eval(&titlenum+1);
                              %end;

/*print summary of univariate analysis*/

                              %if &Nf_uni.^=0 %then 
                              %do;
                                    title1 "0-&titlenum.: List of Statistical Graphics and Tables For Univariate Analysis";
                                    proc print data=_univariate_analysis_(keep=task_id Tablename VariableName Variableformat Analysis_Graphic_type) label;

                                    run;
                                    %let titlenum=%eval(&titlenum+1);
                              %end;

/*print summary of bivariate analysis*/

                              %if &Nf_bi.^=0 %then 

                              %do;
                                    title1 "0-&titlenum.: List of Statistical Graphics and Tables Bivariate Analysis";
                                    proc print data=_bivariate_analysis_(keep=task_id Tablename VariableName1 VariableName2 Variableformat1 Variableformat2 Analysis_Graphic_type)  label;
                                    run;
                                    %let titlenum=%eval(&titlenum+1);
                              %end;

/*print positive pearson table*/
                              %if &Np_pos.^=0 %then 
                              %do;
                                    title1 "0-&titlenum.: Summary of Pearson Correlation Test(Positive) Results";
                                    proc print data=_pearson_positive_  label;run;
                                    %let titlenum=%eval(&titlenum+1);
                              %end;

/*print negative pearson table*/

                              %if &Np_neg.^=0 %then 
                              %do;
                                    title1 "0-&titlenum.: Summary of Pearson Correlation Test(negative) Results";
                                    proc print data=_pearson_negative_  label;run;
                                    %let titlenum=%eval(&titlenum+1);

                              %end;
/*print Chi-SQ test table*/

                              %if &N_chisq.^=0 %then 

                              %do;
                                   title1 "0-&titlenum.: Summary of Chi-Square Test Results";
                                    proc print data=_chisq_ label;run;
                                    %let titlenum=%eval(&titlenum+1);
                              %end;

/*print ANOVA test table*/
                              %if &N_anova.^=0 %then 

                              %do;
                                    title1 "0-&titlenum.: Summary of ANOVA Analysis Results";
                                    proc print data=_anova_(drop=ms ss) label;run;
                                    %let titlenum=%eval(&titlenum+1);
                              %end;
            ods pdf close;
			/*
			ods html close;
			*/
            ods graphics off;
            title;
            footnote;
            ods proctitle;
            ods listing;
      %end;

%let P_criteria=1; 

data _statistical_Test_;
      format Task_id $50.;
      format Table $60.;
      format Variable1 $32.;
      format Variable2 $32.;
      format DF 8.;
      format Statistics 8.2;
      format P_Value PVALUE6.4;
      format Test_Type $50.;
      label Task_id="Task ID";
      label Table="Table";
      label Variable1="Variable 1";
      label Variable2="Variable 2";
      label DF="Degree of Freedom";
      label Statistics="Statistics";
      label P_Value="P Value";
      label Test_Type="Test Type";
      if P_Value^=.;
run;

%if %sysfunc(exist(_pearson_)) %then 
      %do; 
            data _statistical_Test_;
                  set  _Statistical_Test_(in=in1) 
                        _pearson_(in=in2 drop=Use_sample rename=(Pearson_Correlation=Statistics)
                                          where=(P_Value<=&P_criteria.));
                  if in2 then Test_Type="Pearson Correlation Test";
            run;
      %end;

%if %sysfunc(exist(_chisq_)) %then 
      %do; 
            data _statistical_Test_;
                  set  _statistical_Test_(in=in1) 
                        _chisq_(in=in2 rename=(DoF=DF Chi_SQ=Statistics)
                                    where=(P_Value<=&P_criteria.));
                  if in2 then Test_Type="Chi-SQ Test";
            run;
      %end;

%if %sysfunc(exist(_anova_)) %then 
      %do; 
            data _statistical_Test_;
                  set  _statistical_Test_(in=in1) 
                        _anova_(in=in2 drop=SS MS 
                                    rename=(Dependent=Variable1 Group=Variable2 FValue=Statistics ProbF=P_Value)
                                    where=(P_Value<=&P_criteria.));
                  if in2 then Test_Type="ANOVA";
            run;
      %end;

data _statistical_Test_;
      set _statistical_Test_;
      format vartmp $32.;
      Table=Propcase(Table);
      Variable1=Propcase(Variable1);
      Variable2=propcase(Variable2);
      if variable1>Variable2 then
            do;
                  vartmp=variable1;
                  variable1=variable2;
                  variable2=vartmp;
            end;
      drop vartmp;
run;

data _statistical_test_;
	set _statistical_test_;
	format P_value_star $5.;
	if P_value>=0 then
		do;
			if P_value<=0.01 then P_value_star='***';
				else if P_value<=0.05 then P_value_star='**';
				else if P_value<=0.1 then P_value_star='*';
		end;
run;

proc sort data=_statistical_Test_;
      by table variable1 variable2;
run;

%if %sysfunc(exist(_all_ext_)) %then %do; proc sql noprint; drop table _all_ext_; quit; %end;
%if %sysfunc(exist(_all_var_infor_)) %then %do; proc sql noprint; drop table _all_var_infor_; quit; %end;
%if %sysfunc(exist(_anova_)) %then %do; proc sql noprint; drop table _anova_; quit; %end;
%if %sysfunc(exist(_bivariable_exclude_reason_)) %then %do; proc sql noprint; drop table _bivariable_exclude_reason_; quit; %end;
%if %sysfunc(exist(_bivariate_)) %then %do; proc sql noprint; drop table _bivariate_; quit; %end;
%if %sysfunc(exist(_bivariate_analysis_)) %then %do; proc sql noprint; drop table _bivariate_analysis_; quit; %end;
%if %sysfunc(exist(_bivariate_analysis_in_)) %then %do; proc sql noprint; drop table _bivariate_analysis_in_; quit; %end;
%if %sysfunc(exist(_bivariate_pair_exclude_)) %then %do; proc sql noprint; drop table _bivariate_pair_exclude_; quit; %end;
%if %sysfunc(exist(_chisq_)) %then %do; proc sql noprint; drop table _chisq_; quit; %end;
%if %sysfunc(exist(_exclude_list_)) %then %do; proc sql noprint; drop table _exclude_list_; quit; %end;
%if %sysfunc(exist(_graphic_type_bi_)) %then %do; proc sql noprint; drop table _graphic_type_bi_; quit; %end;
%if %sysfunc(exist(_graphic_type_uni_)) %then %do; proc sql noprint; drop table _graphic_type_uni_; quit; %end;
%if %sysfunc(exist(_pearson_)) %then %do; proc sql noprint; drop table _pearson_; quit; %end;
%if %sysfunc(exist(_pearson_negative_)) %then %do; proc sql noprint; drop table _pearson_negative_; quit; %end;
%if %sysfunc(exist(_pearson_positive_)) %then %do; proc sql noprint; drop table _pearson_positive_; quit; %end;
%if %sysfunc(exist(_univariate_)) %then %do; proc sql noprint; drop table _univariate_; quit; %end;
%if %sysfunc(exist(_univariate_analysis_)) %then %do; proc sql noprint; drop table _univariate_analysis_; quit; %end;
%if %sysfunc(exist(_univariate_analysis_in_)) %then %do; proc sql noprint; drop table _univariate_analysis_in_; quit; %end;
%if %sysfunc(exist(_variable_exclude_reason_)) %then %do; proc sql noprint; drop table _variable_exclude_reason_; quit; %end;
%if %sysfunc(exist(_variable_include_format_)) %then %do; proc sql noprint; drop table _variable_include_format_; quit; %end;
%if %sysfunc(exist(_variable_list_)) %then %do; proc sql noprint; drop table _variable_list_; quit; %end;
%if %sysfunc(exist(_variable_list_dist_)) %then %do; proc sql noprint; drop table _variable_list_dist_; quit; %end;
%if %sysfunc(exist(_var_exclude_)) %then %do; proc sql noprint; drop table _var_exclude_; quit; %end;
%if %sysfunc(exist(_var_include_)) %then %do; proc sql noprint; drop table _var_include_; quit; %end;
%if %sysfunc(exist(_var_infor_)) %then %do; proc sql noprint; drop table _var_infor_; quit; %end;
%if %sysfunc(exist(_var_infor_dist)) %then %do; proc sql noprint; drop table _var_infor_dist; quit; %end;
%if %sysfunc(exist(_bi_runtime_)) %then %do; proc sql noprint; drop table _bi_runtime_; quit; %end;
%if %sysfunc(exist(_executive_summary_)) %then %do; proc sql noprint; drop table _executive_summary_; quit; %end;
%if %sysfunc(exist(_uni_runtime_)) %then %do; proc sql noprint; drop table _uni_runtime_; quit; %end;

%if %sysfunc(exist(_BI_LIB_)) %then %do; proc sql noprint; drop table _BI_LIB_; quit; %end;
%if %sysfunc(exist(_BI_LIB_DIST_)) %then %do; proc sql noprint; drop table _BI_LIB_DIST_; quit; %end;
%if %sysfunc(exist(_UNI_LIB_)) %then %do; proc sql noprint; drop table _UNI_LIB_; quit; %end;
%if %sysfunc(exist(_UNI_LIB_DIST_)) %then %do; proc sql noprint; drop table _UNI_LIB_DIST_; quit; %end;



%if %length(&Nf_Uni)=0 %then %let Nf_Uni=0;
%if %length(&N_bi)=0 %then %let N_bi=0;
%put The analysis started at &dt_start., and ended at &dt_end.;
%put It took &run_time. to generate %eval(&N_Bi.+&N_Uni.) statistical graphics.;
%put ~~~Thanks for doing your analysis in a smart way!~~~;

%mend;


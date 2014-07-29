package com.sukasa.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class QuartzJob implements Job {

    @SuppressWarnings("unused")
	public void execute(JobExecutionContext context)
            throws JobExecutionException {
       // System.out.println("La gente esta muy loca");
        
       /*StringBuilder resultadoMYSQL=ProcedimientoMySQL.procesarMYSQL(null);
       System.out.println("RESULTADO MYSQL-------------->"+resultadoMYSQL);*/
       StringBuilder resultadoORACLE=ProcedimientoOracle.procesarORACLE(null);
      // System.out.println("RESULTADO ORACLE-------------->"+resultadoORACLE);
    }

}

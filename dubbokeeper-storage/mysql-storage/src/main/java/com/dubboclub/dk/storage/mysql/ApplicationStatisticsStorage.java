package com.dubboclub.dk.storage.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.dubboclub.dk.storage.model.ApplicationInfo;
import com.dubboclub.dk.storage.model.Statistics;
import com.dubboclub.dk.storage.mysql.mapper.ApplicationMapper;
import com.dubboclub.dk.storage.mysql.mapper.StatisticsMapper;

/**
 * @date: 2015/12/28.
 * @author:bieber.
 * @project:dubbokeeper.
 * @package:com.dubboclub.dk.storage.mysql.
 * @version:1.0.0
 * @fix:
 * @description: 描述功能
 */
public class ApplicationStatisticsStorage {

    private static final String APPLICATION_TEMPLATE="CREATE TABLE `statistics_{}` (\n" +
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "  `timestamp` bigint(1) NOT NULL DEFAULT '0' COMMENT '时间戳',\n" +
            "  `serviceInterface` varchar(255) NOT NULL DEFAULT '' COMMENT '接口名',\n" +
            "  `method` varchar(255) NOT NULL DEFAULT '' COMMENT '方法名',\n" +
            "  `type` varchar(10) DEFAULT NULL COMMENT '当前调用的应用类型',\n" +
            "  `tps` float(11,2) NOT NULL DEFAULT '0.00' COMMENT 'TPS值',\n" +
            "  `kbps` float(11,2) DEFAULT NULL COMMENT '流量',\n" +
            "  `host` varchar(50) DEFAULT NULL COMMENT 'ip地址',\n" +
            "  `elapsed` int(11) DEFAULT NULL COMMENT '耗时',\n" +
            "  `concurrent` int(11) DEFAULT NULL COMMENT '并发数',\n" +
            "  `input` int(11) DEFAULT NULL COMMENT '输入值',\n" +
            "  `output` int(11) DEFAULT NULL COMMENT '输出大小',\n" +
            "  `successCount` int(11) DEFAULT NULL COMMENT '成功次数',\n" +
            "  `failureCount` int(11) DEFAULT NULL COMMENT '失败次数',\n" +
            "  `remoteAddress` varchar(50) DEFAULT NULL COMMENT '远程地址',\n" +
            "  `remoteType` varchar(20) DEFAULT NULL COMMENT '远程应用类型',\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  KEY `time-index` (`timestamp`),\n" +
            "  KEY `method-index` (`method`),\n" +
            "  KEY `service-index` (`serviceInterface`)\n"+
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

    private StatisticsMapper statisticsMapper;

    private DataSource dataSource;

    private TransactionTemplate transactionTemplate;

    private ApplicationMapper applicationMapper;

    private LinkedBlockingDeque<Statistics> statisticsCollection;

    private String application;

    private volatile long maxElapsed;

    private volatile long maxConcurrent;

    private volatile int maxFault;

    private volatile int maxSuccess;

    private int type;

    private static final int WRITE_INTERVAL= Integer.parseInt(ConfigUtils.getProperty("mysql.commit.interval", "1000"));
    
    private static final int MAX_BATCH_NUM = Integer.parseInt(ConfigUtils.getProperty("mysql.commit.maxBatchNum", "1000"));
    
    private static final int MAX_QUEUE_NUM = Integer.parseInt(ConfigUtils.getProperty("dubbo.monitor.maxQueueNum", "100000"));
    
    private ScheduledExecutorService scheduledExecutorService;


    public ApplicationStatisticsStorage(ApplicationMapper applicationMapper,StatisticsMapper statisticsMapper,DataSource dataSource,TransactionTemplate transactionTemplate,String application,int type){
        this(applicationMapper,statisticsMapper,dataSource,transactionTemplate,application,type,false);
    }

    public ApplicationStatisticsStorage(ApplicationMapper applicationMapper,StatisticsMapper statisticsMapper,DataSource dataSource,TransactionTemplate transactionTemplate,String application,int type,boolean needCreateTable){
        this.application = application;
        this.statisticsMapper = statisticsMapper;
        this.dataSource = dataSource;
        this.transactionTemplate = transactionTemplate;
        this.applicationMapper = applicationMapper;
        if(needCreateTable){
            ApplicationInfo applicationInfo = new ApplicationInfo();
            applicationInfo.setApplicationName(application);
            applicationInfo.setApplicationType(type);
            this.applicationMapper.addApplication(applicationInfo);
            createNewAppTable(application);
        }
        init();
        this.type=type;
        statisticsCollection = new LinkedBlockingDeque<Statistics>(MAX_QUEUE_NUM);
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }


    private void init(){
        long end = System.currentTimeMillis();
        long start = System.currentTimeMillis()-24*60*60*1000;
        Long concurrent =statisticsMapper.queryMaxConcurrent(application,null,start,end);
        Long elapsed = statisticsMapper.queryMaxElapsed(application,null,start,end);
        Integer fault = statisticsMapper.queryMaxFault(application,null,start,end);
        Integer success = statisticsMapper.queryMaxSuccess(application,null,start,end);
        maxConcurrent =concurrent==null?0:concurrent;
        maxElapsed = elapsed==null?0:elapsed;
        maxFault=fault==null?0:fault;
        maxSuccess=success==null?0:success;
    }

    public void addStatistics(Statistics statistics){
        if(WRITE_INTERVAL<=0){
            statisticsMapper.addOne(application,statistics);
        }else{
            boolean result = statisticsCollection.offer(statistics);
            if(!result){
            	// 如果队列数据满了，需要从头部去除一个然后从尾部再添加 Dimmacro
            	statisticsCollection.poll();
            	statisticsCollection.offer(statistics);
            }
        }
        if(maxFault<statistics.getFailureCount()){
            maxFault=statistics.getFailureCount();
        }
        if(maxSuccess<statistics.getSuccessCount()){
            maxSuccess=statistics.getSuccessCount();
        }
        if(maxConcurrent<statistics.getConcurrent()){
            maxConcurrent = statistics.getConcurrent();
        }
        if(maxElapsed<statistics.getElapsed()){
            maxElapsed = statistics.getElapsed();
        }
        if((type==0&&statistics.getType()== Statistics.ApplicationType.PROVIDER)||(type==1&&statistics.getType()== Statistics.ApplicationType.CONSUMER)){
            synchronized (this){
                if(type!=2){
                    applicationMapper.updateAppType(application,2);
                    type=2;
                }
            }
        }
    }
    
    public void startSaveData() {
		scheduledExecutorService.scheduleWithFixedDelay(new Thread(this.application+ "_statisticsWriter") {
			
			@Override
			public void run() {
				List<Statistics> statisticsList = new ArrayList<Statistics>();
	            Statistics statistics;
	            int num = 0;
	            while(num++ < MAX_BATCH_NUM && null != (statistics = statisticsCollection.poll())){
	            	statisticsList.add(statistics);
	            }
	            if(statisticsList.size()>0){
	                batchInsert(statisticsList);
	             }
				
			}
			
			public boolean batchInsert(final List<Statistics> statisticsList){
		        return transactionTemplate.execute(new TransactionCallback<Boolean>() {
		            @Override
		            public Boolean doInTransaction(TransactionStatus status) {
		                int size = statisticsMapper.batchInsert(application,statisticsList);
		                if(size!=statisticsList.size()){
		                    status.setRollbackOnly();
		                    return false;
		                }
		                return true;
		            }
		        });
		    }
		}, WRITE_INTERVAL, WRITE_INTERVAL, TimeUnit.MILLISECONDS);
		
	}
    
    private boolean  createNewAppTable(final String applicationName){
        return transactionTemplate.execute(new TransactionCallback<Boolean>() {
            @Override
            public Boolean doInTransaction(TransactionStatus status) {
                Connection connection = DataSourceUtils.getConnection(dataSource);
                String tableSql = StringUtils.replace(APPLICATION_TEMPLATE,"{}",applicationName);
                Statement statement=null;
                try {
                    statement = connection.createStatement();
                    statement.execute(tableSql);
                } catch (SQLException e) {
                    e.printStackTrace();
                    status.setRollbackOnly();
                    return false;
                }finally {
                    if(statement!=null){
                        try {
                            statement.close();
                        } catch (SQLException e) {
                            //do nothing
                        	e.printStackTrace();
                        }
                    }
                }
                return true;
            }
        });
    }


    public int getMaxSuccess() {
        return maxSuccess;
    }

    public int getMaxFault() {
        return maxFault;
    }

    public long getMaxConcurrent() {
        return maxConcurrent;
    }

    public long getMaxElapsed() {
        return maxElapsed;
    }

    public String getApplication() {
        return application;
    }

    public int getType() {
        return type;
    }

}

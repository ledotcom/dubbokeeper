/**
 * Project: dubbo.registry-1.1.0-SNAPSHOT
 *
 * File Created at 2010-4-9
 * $Id: Provider.java 182846 2012-06-28 09:37:59Z tony.chenl $
 *
 * Copyright 2008 Alibaba.com Croporation Limited.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Alibaba Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Alibaba.com.
 */
package com.dubboclub.dk.admin.model;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.dubboclub.dk.admin.sync.util.Tool;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Provider
 *
 * @author william.liangf
 * @author tony.chenl
 */
public class Provider extends BasicModel {

    private static final long serialVersionUID = 5981342400350878171L;

    private String serviceKey;/* 提供者所提供的服务名称 */

    private String url; /* 提供者提供服务的地址 */

    private String parameters; /* 提供者提供服务的参数 */

    private String address; /* 提供者地址 */

    private String registry;/* 提供者连接的注册中心地址 */

    private boolean dynamic;          /* 是否为动态注册服务 */

    private boolean enabled;          /* 是否启用 */

    private int weight;          /* 权重 */

    private String application; /* 应用名 */

    private String username;      /* 提供者用户名 */

    private Date expired;   /*过期时间*/

	private long alived;    /*存活时间，单位秒*/
    
    // add by Dimmacro 2016年4月5日14:46:26
    private String standardConf ; /* 标准参数 */


    private String group;

    private String version;

    private Override override;

    private List<Override> overrides;


    public Provider() {
    }


    public Provider(Long id) {
        super(id);
    }


    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getServiceKey() {
        return serviceKey;
    }

    public void setServiceKey(String serviceKey) {
        this.serviceKey = serviceKey;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getParameters() {
        return parameters;
    }

    public void setParameters(String parameters) {
        this.parameters = parameters;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getRegistry() {
        return registry;
    }

    public void setRegistry(String registry) {
        this.registry = registry;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }


    public Date getExpired() {
        return expired;
    }


    public void setExpired(Date expired) {
        this.expired = expired;
    }

    public long getAlived() {
        return alived;
    }

    public void setAlived(long aliveSeconds) {
        this.alived = aliveSeconds;
    }
    

    public String getStandardConf() {
		return standardConf;
	}


	public void setStandardConf(String standardConf) {
		this.standardConf = standardConf;
	}

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public Override getOverride() {
        return override;
    }

    public void setOverride(Override override) {
        this.override = override;
    }

    public List<Override> getOverrides() {
        return overrides;
    }

    public void setOverrides(List<Override> overrides) {
        this.overrides = overrides;
    }

    public URL toUrl() {
        Map<String, String> serviceName2Map = Tool.serviceName2Map(getServiceKey());


        String u = getUrl();
        URL url = URL.valueOf(u + "?" + getParameters());

        url = url.addParameters(serviceName2Map);

        boolean dynamic = isDynamic();
        if (!dynamic) {
            url = url.addParameter(Constants.DYNAMIC_KEY, false);
        }
        boolean enabled = isEnabled();
        if (enabled != url.getParameter("enabled", true)) {
            if (enabled) {
                url = url.removeParameter("enabled");
            } else {
                url = url.addParameter("enabled", false);
            }
        }

        return url;
    }

}

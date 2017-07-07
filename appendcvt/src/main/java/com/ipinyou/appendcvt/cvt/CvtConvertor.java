package com.ipinyou.appendcvt.cvt;

import com.ipinyou.appendcvt.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by lance on 2017/7/6.
 */
public class CvtConvertor {
    private static final Logger logger = LoggerFactory.getLogger(CvtConvertor.class);

    private String inPath;
    private String outPath;
    private String backupDir = null;

    public CvtConvertor(String inPath, String outPath, String backupDir) {
        if (inPath == null || "".equals(inPath) ||
                outPath == null || "".equals(outPath)) {
            throw new IllegalArgumentException("invalid Arguments");
        }
        this.inPath = FileUtils.getAbsPath(inPath);
        this.outPath = FileUtils.getAbsPath(outPath);
        this.backupDir = backupDir;
    }

    public boolean convert() {
        logger.info("Start convert from[{}] to[{}] start", inPath, outPath);

        if (!FileUtils.isFile(inPath)) {
            logger.error("Invalid inpath[{}] not file", inPath);
            return false;
        }

        if (!FileUtils.deleteFileIfExists(outPath)) {
            logger.error("Delete exists file[{}] failed", outPath);
            return false;
        }

        try {
            BufferedReader br = new BufferedReader(new FileReader(inPath));
            BufferedWriter writer = new BufferedWriter(new FileWriter(outPath));
            String line = null;
            while ((line = br.readLine()) != null) {
                String result = convertLine(line);
                if (result == null) {
                    logger.warn("convertLine line[{}] failed", line);
                    continue;
                }
                writer.write(result);
            }
            writer.close();
            br.close();

            if (backupDir != null) {
                String backPath = backupDir + "/" + FileUtils.getFileName(inPath);
                backPath = FileUtils.renameFileWithTimestamp(inPath, backPath);
                if (backPath == null) {
                    logger.error("renameFileWithTimestamp from[{}] to[{}] failed", inPath, backupDir);
                } else {
                    logger.info("renameFileWithTimestamp from[{}] to[{}] success", inPath, backPath);
                }
            } else {
                if (!FileUtils.deleteFileIfExists(inPath)) {
                    logger.error("Delete exists file[{}] failed", inPath);
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public static String convertLine(String line) {
        String[] cvt = line.split("\t",-1);
        if (cvt.length < 72) {
            return null;
        }

        StringBuilder result = new StringBuilder();
        String partner_id = "";
        result.append(partner_id+"\t");
        String advertiser_company_id = cvt[50];
        result.append(advertiser_company_id+"\t");
        String advertiser_id = "";
        result.append(advertiser_id+"\t");
        String order_id = "";
        result.append(order_id+"\t");
        String campaign_id = "";
        result.append(campaign_id+"\t");
        String sub_campaign_id = "";
        result.append(sub_campaign_id+"\t");
        String exe_campaign_id = "";
        result.append(exe_campaign_id+"\t");
        String vertical_tag_id = "";
        result.append(vertical_tag_id+"\t");
        String conversion_pixel = cvt[51];
        result.append(conversion_pixel+"\t");
        String creative_size = "";
        result.append(creative_size+"\t");
        String creative_id = cvt[53];
        result.append(creative_id+"\t");
        String creative_type = "";
        result.append(creative_type+"\t");
        String country_id = "";
        result.append(country_id+"\t");
        String province_id = "";
        result.append(province_id+"\t");
        String city_id = "";
        result.append(city_id+"\t");
        String inventory_type = "";
        String device_type = cvt[41];
        String agent_type = cvt[24];
        if(device_type.equals("General")){
            inventory_type = "PC";
        }else if(agent_type.equals("Browser")){
            inventory_type = "Mob_web";
        }else{
            inventory_type = "Mob_app";
        }
        result.append(inventory_type+"\t");
        String adslot_type = cvt[62];
        result.append(adslot_type+"\t");
        String banner_view_type = "";
        result.append(banner_view_type+"\t");
        String video_view_type = "";
        result.append(video_view_type+"\t");
        String native_view_type = "";
        result.append(native_view_type+"\t");
        String ad_unit_id = cvt[55];
        result.append(ad_unit_id+"\t");
        String ad_unit_width = "";
        result.append(ad_unit_width+"\t");
        String ad_unit_height = "";
        result.append(ad_unit_height+"\t");
        String platform = cvt[3];
        result.append(platform+"\t");
        String domain_category = "";
        result.append(domain_category+"\t");
        String top_level_domain = GetTopDomain.getTopDomain(cvt[21]);
        result.append(top_level_domain+"\t");
        String domain = GetTopDomain.getDomain(cvt[21]);
        result.append(domain+"\t");
        String app_category = "";
        result.append(app_category+"\t");
        String app_name = "";
        result.append(app_name+"\t");
        String app_id = cvt[27];
        result.append(app_id+"\t");
        String content_vertical_first = "";
        result.append(content_vertical_first+"\t");
        String content_title = "";
        result.append(content_title+"\t");
        String deal_type = "";
        result.append(deal_type+"\t");
        String deal_id = "";
        result.append(deal_id+"\t");
        result.append(device_type+"\t");
        String os = cvt[42];
        result.append(os+"\t");
        String browser = "";
        result.append(browser+"\t");
        String brand = cvt[43];
        result.append(brand+"\t");
        String model = cvt[44];
        result.append(model+"\t");
        String networkgeneration = "";
        result.append(networkgeneration+"\t");
        String carrier = cvt[45];
        result.append(carrier+"\t");
        String mob_device_type = "";
        if(!device_type.equals("General")){
            mob_device_type = device_type;
        }
        result.append(mob_device_type+"\t");
        String mac = "";
        result.append(mac+"\t");
        String mac_enc = "";
        result.append(mac_enc+"\t");
        String imei = "";
        result.append(imei+"\t");
        String imei_enc = "";
        result.append(imei_enc+"\t");
        String imsi = "";
        result.append(imsi+"\t");
        String imsi_enc = "";
        result.append(imsi_enc+"\t");
        String dpid = "";
        result.append(dpid+"\t");
        String dpid_enc = "";
        result.append(dpid_enc+"\t");
        String adid = "";
        result.append(adid+"\t");
        String adid_enc = "";
        result.append(adid_enc+"\t");
        String pyid = "";
        result.append(pyid+"\t");
        String daat = "";
        result.append(daat+"\t");
        String imp_request_time = "";
        result.append(imp_request_time+"\t");
        String clk_request_time = "";
        result.append(clk_request_time+"\t");
        String cvt_request_time = cvt[6];
        result.append(cvt_request_time+"\t");
        String session_id = cvt[5];
        result.append(session_id+"\t");
        String money = cvt[64];
        result.append(money+"\t");
        String orderno = cvt[65];
        result.append(orderno+"\t");
        String product_list = cvt[66];
        result.append(product_list+"\t");
        String cvt_url = cvt[21];
        result.append(cvt_url+"\t");
        String cvt_refer_url = cvt[23];
        result.append(cvt_refer_url+"\t");
        String imp_url = "";
        result.append(imp_url+"\t");
        String clk_url = "";
        result.append(clk_url+"\t");
        String pday = cvt_request_time.substring(0,8);
        result.append(pday+"\t");
        String cvt_action_id = cvt[0];
        result.append(cvt_action_id+"\t");
        String phour = cvt_request_time.substring(8,10);
        result.append(phour+"\t");
        String spamlevel = "";
        result.append(spamlevel+"\t");
        String bidmodule = "";
        result.append(bidmodule+"\t");
        String bidmoduleversion = "";
        result.append(bidmoduleversion+"\t");
        String randmodule = "";
        result.append(randmodule+"\t");
        String randmoduleversion = "";
        result.append(randmoduleversion+"\t");
        String creativedecisionmodule = "";
        result.append(creativedecisionmodule+"\t");
        String creativedecisionmoduleversion = "";
        result.append(creativedecisionmoduleversion+"\t");
        String algobidtag1 = "";
        result.append(algobidtag1+"\t");
        String algobidtag2 = "";
        result.append(algobidtag2+"\t");
        String cvtpyid = cvt[15];
        result.append(cvtpyid+"\t");
        result.append("\n");

        return result.toString();
    }
}

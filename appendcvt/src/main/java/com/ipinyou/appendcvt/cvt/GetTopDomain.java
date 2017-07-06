package com.ipinyou.appendcvt.cvt;

import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by lance on 2017/7/6.
 */
public class GetTopDomain {
    private static String[] domainExt = {"com","net","org","gov","edu","info","name","int","mil","arpa","asia","biz","pro","coop","aero",
            "museum","ac","ad","ae","af","ag","ai","al","am","an","ao","aq","ar","as","at","au","aw","az","ba","bb","bd","be",
            "bf","bg","bh","bi","bj","bm","bn","bo","br","bs","bt","bv","bw","by","bz","ca","cc","cf","cg","ch","ci","ck","cl",
            "cm","cn","co","cq","cr","cu","cv","cx","cy","cz","de","dj","dk","dm","do","dz","ec","ee","eg","eh","es","et","ev",
            "fi","fj","fk","fm","fo","fr","ga","gb","gd","ge","gf","gh","gi","gl","gm","gn","gp","gr","gt","gu","gw","gy","hk",
            "hm","hn","hr","ht","hu","id","ie","il","in","io","iq","ir","is","it","jm","jo","jp","ke","kg","kh","ki","km","kn",
            "kp","kr","kw","ky","kz","la","lb","lc","li","lk","lr","ls","lt","lu","lv","ly","ma","mc","md","me","mg","mh","ml",
            "mm","mn","mo","mp","mq","mr","ms","mt","mv","mw","mx","my","mz","na","nc","ne","nf","ng","ni","nl","no","np","nr",
            "nt","nu","nz","om","pa","pe","pf","pg","ph","pk","pl","pm","pn","pr","pt","pw","py","qa","re","ro","ru","rw","sa",
            "sb","sc","sd","se","sg","sh","si","sj","sk","sl","sm","sn","so","sr","st","su","sy","sz","tc","td","tf","tg","th",
            "tj","tk","tm","tn","to","tp","tr","tt","tv","tw","tz","ua","ug","uk","us","uy","va","vc","ve","vg","vn","vu","wf",
            "ws","ye","yu","za","zm","zr","zw","html","shtml"};
    private static Set<String> domainExts = new HashSet<String>(Arrays.asList(domainExt));

    public static String getDomain(String url) {
        if(url.length() ==0){
            return "";
        }
        String url_s = "";
        try{
            url_s = URLDecoder.decode(url,"utf-8");
        }
        catch (Exception e){

        }
        String domain = "";
        int startIndex = -1;
        int endIndex = -1;
        startIndex = url_s.indexOf("//");
        if(startIndex == -1){
            startIndex = 0;
        }else{
            startIndex +=2;
        }
        int tmpIndex = url_s.indexOf("/",startIndex);
        int tmpIndex1 = url_s.indexOf("?",startIndex);
        if(tmpIndex == -1){
            tmpIndex = Integer.MAX_VALUE;
        }else{
            tmpIndex = tmpIndex;
        }
        if(tmpIndex1 == -1){
            tmpIndex1 = Integer.MAX_VALUE;
        }else{
            tmpIndex1 = tmpIndex1;
        }
        endIndex = Math.min(tmpIndex,tmpIndex1);
        if(endIndex == Integer.MAX_VALUE){
            domain = url_s.substring(startIndex);
        }else{
            if(endIndex > startIndex){
                domain = url_s.substring(startIndex,endIndex);
            }else{
                domain = "";
            }
        }
        return domain.trim();

    }

    public static String getTopDomain(String url){
        String domain = getDomain(url);
        if(domain.contains(":")){
            domain = domain.split(":")[0];
        }
        if(domain.length() == 0){
            return "";
        }
        String maindomain = "";
        String[] domainParts = domain.split("[.]");
        if(domainParts.length < 2){
            return "";
        }

        for(int i=domainParts.length-1;i>=0;i--){
            if(domainExts.contains(domainParts[i]))
                continue;
            for(int j=i; j<domainParts.length; j++){
                maindomain += "."+domainParts[j];
            }
            break;
        }
        maindomain = maindomain.length()>0 ? maindomain.substring(1) : "";

        return maindomain;
    }
}

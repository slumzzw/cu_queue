<?php
/**
 * @file ToolArray.php
 * @synopsis  数组相关处理
 * @author zhoubinbin, zhoubinbin@moyi365.com
 * @version 1
 * @date 2015-06-23
 */
namespace Javion\cu_queue\Lib;

class ToolArray{
    //value(array_column())
    public static function getFieldValue($arrData, $strKey='id', $bolUnique = true) {
        $arrValue = array();
        if(!is_array($arrData) || !count($arrData)) {
            return $arrValue;
        }
        if (function_exists('array_column')){
            $arrValue = array_column($arrData, $strKey);
        }else{
            foreach($arrData as $key=>$val) {
                if(isset($val[$strKey])) {
                    $arrValue[] = $val[$strKey];
                }
            }
        }

        if($bolUnique) {
            $arrValue = array_unique($arrValue);
        }

        return $arrValue;
    }

    public static function getMapFromList($arrData, $strKey='id') {
        $arrValue = array();

        if(!is_array($arrData) || !count($arrData)) {
            return $arrValue;
        }

        foreach($arrData as $val) {
            if(isset($val[$strKey])) {
                $arrValue[$val[$strKey]] = $val;
            }
        }

        return $arrValue;
    }

    //map:key->value(array_column())
    public static function getMapFieldValue($arrList, $strField, $strKey='id') {
        $arrData = array();
        if(!is_array($arrList) || empty($arrList)) {
            return $arrData;
        }

        if (function_exists('array_column')){
            $arrData = array_column($arrList, $strField, $strKey);
        }else{
            foreach($arrList as $val) {
                if(isset($val[$strKey]) && isset($val[$strField])) {
                    $arrData[$val[$strKey]] = $val[$strField];
                }
            }
        }

        return $arrData;
    }
}

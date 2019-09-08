using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using System.Dynamic;

namespace Pers_Control_data
{
    public class Per_ctrl
    {
        public int instances; // zmienna do kontoli wykonania (tylko jedno połączenie z ORACLE)
        public async Task<object> Invoke(IDictionary<string, object> parameters)
        {
            try
            {
                string type_dta = ((string)parameters["type_dta"]);
                string pers_id = ((string)parameters["person_id"]);
                string mnth = ((string)parameters["mnth"]);
                if (mnth.Length > 6) { throw new Exception("Argument mnth musi być w formacie YYYYMM"); }
                
                if (type_dta == "Pers_ctrl")
                {
                    return await Person_control(pers_id, mnth);
                }
                else if (type_dta == "MNTH_control")
                {
                    return await List_control(mnth);
                }
                else if (type_dta == "Kontrol_stat")
                {
                    return await Kotrl_stat(mnth);
                }
                else if (type_dta == "Lst_ctrl_err")
                {
                    return await Kotrl_ERR_lst(mnth, pers_id);
                }
                else if (type_dta == "Ctrl_detalis")
                {
                    return await Control_detalis(pers_id);
                }
                else if (type_dta == "Emp_Ctrl_sta")
                {
                    return await Rows_Ctrl_stat(mnth, pers_id);
                }
                else if (type_dta == "EMP_summ_sta")
                {
                    return await Emp_ctrl_summ(mnth);
                }
                else if (type_dta == "PER_ctrl_enh")
                {
                    return await Person_control_E(pers_id, mnth);
                }
                else
                {
                    throw new Exception("Nieznana Komenda :" + type_dta);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Błąd inicjalizacji :", e);
            }
        }
        public async Task<Ctr_rek_E> Person_control_E(string per_id, string mnth)
        {
            try
            {
                while (instances > 0)
                {
                    if (instances > 0) { System.Threading.Thread.Sleep(300); }
                }
                instances = instances + 1;
                var main_rek = new Ctr_rek_E();
                var rek_data = new List<Emp_ctrl_E>();
                string _EMP_NO = " ";
                string _EMP_ID = " ";
                string _EMP_NAME = " ";
                double numrek = 0;
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {

                    using (OracleCommand kal = new OracleCommand("select a.EMP_NO,ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO) EMP_ID,ifsapp.person_info_api.Get_Name(ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO)) EMP_Name,Decode(a.Q_C_STATUS_DB,'TRUE',1,DEcode(nvl(IFSAPP.C_SHOP_ORD_QPC_LN_API.Get_Sum_Factor( a.ID_QPC ),0),0 ,0,1,1,2,2,3,3,3)*-3) * a.QUANTITY Punkty_zlec,0 PUNKTY,a.ID_QPC ,a.ORDER_NO,ifsapp.shop_ord_api.Get_Part_No(a.ORDER_NO,a.RELEASE_NO,a.SEQUENCE_NO) part_no,ifsapp.inventory_part_api.Get_Description('ST',ifsapp.shop_ord_api.Get_Part_No(a.ORDER_NO,a.RELEASE_NO,a.SEQUENCE_NO)) descr,Q_C_DATE,a.QUANTITY,a.OPERATION_NO,a.CONTROLER_PERSON,ifsapp.person_info_api.Get_Name(a.CONTROLER_PERSON) Controler_name,a.Q_C_STATUS,Decode(b.order_no,a.order_no,'Zgłoszony','Nie wykonywał') valid FROM IFSAPP.C_SHOP_ORD_QPC_HD a left JOIN (SELECT a.order_no,b.emp_no FROM ifsapp.operation_history a ,ifsapp.c_work_group_det b WHERE a.DATE_APPLIED>=To_Date(:mnth,'YYYYMM')-30 AND b.VALID_DATE>=To_Date(:mnth,'YYYYMM')-30 AND b.MEMBER_WORK_STATE_DB IN ('W','O') AND  b.WORK_GROUP_ID=Nvl(a.C_WORK_GROUP_ID,a.BRIGADE_NO) AND a.DATE_APPLIED=b.VALID_DATE GROUP BY a.order_no,b.emp_no) b ON b.order_no= a.order_no AND b.emp_no=a.EMP_NO WHERE a.EMP_NO=:emp_no and to_char(a.Q_C_DATE,'YYYYMM')=:mnth Order by  a.Q_C_DATE,a.objversion", conO))
                    {
                        await conO.OpenAsync();
                        kal.BindByName = true;
                        kal.Parameters.Add(new OracleParameter(":emp_no", OracleDbType.Varchar2) { Value = per_id });
                        kal.Parameters.Add(new OracleParameter(":mnth", OracleDbType.Varchar2) { Value = mnth });
                        using (var reader = await kal.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                        {
                            double points = 0;
                            double ctr_point = 0;
                            double prog = 0;
                            double premia = 0;
                            int i = 0;
                            while (await reader.ReadAsync())
                            {
                                numrek = numrek + 1;
                                if (i == 0)
                                {
                                    _EMP_NO = reader.GetString(0);
                                    _EMP_ID = reader.GetString(1);
                                    _EMP_NAME = reader.GetString(2);
                                    i = i + 1;
                                }
                                ctr_point = reader.GetDouble(3);
                                points = points + ctr_point;
                                if (points >= 50)
                                {
                                    points = 50;
                                    prog = 5;
                                    premia = 110;
                                }
                                else if (points < -30)
                                {
                                    points = -30;
                                    prog = 0;
                                    premia = 0;
                                }
                                else if (points < 50 & points >= 31)
                                {
                                    prog = 4;
                                    premia = 105;
                                }
                                else if (points < 31 & points >= 0)
                                {
                                    prog = 3;
                                    premia = 100;
                                }
                                else if (points < 0 & points >= -10)
                                {
                                    prog = 2;
                                    premia = 80;
                                }
                                else if (points < -10 & points > -30)
                                {
                                    prog = 1;
                                    premia = points * 2 + 100;
                                }
                                var rek = new Emp_ctrl_E
                                {
                                    NUM_REK = numrek,
                                    VALID = reader.GetString(15),
                                    PUNKTY_zlec = reader.GetDouble(3),
                                    ORDER_NO = reader.GetString(6),
                                    PART_NO = reader.GetString(7),
                                    DESCR = reader.GetString(8),
                                    Q_C_DATE = reader.GetDateTime(9).ToString(),
                                    QUANTITY = reader.GetDouble(10),
                                    CNTR_PERSON = reader.GetString(12),
                                    CNTR_NAME = reader.GetString(13),
                                    Q_C_STATUS = reader.GetString(14)
                                };
                                rek_data.Add(rek);
                            }
                            if (i != 0)
                            {
                                var re = new Ctr_rek_E
                                {
                                    DATA_VALID = "true",
                                    MNTH = mnth,
                                    EMP_NO = _EMP_NO,
                                    EMP_ID = _EMP_ID,
                                    EMP_NAME = _EMP_NAME,
                                    PUNKTY = points,
                                    PROG = prog,
                                    PREMIA = premia,
                                    CTRL_REK = rek_data
                                };
                                main_rek = re;
                            }
                            else
                            {
                                var re = new Ctr_rek_E
                                {
                                    DATA_VALID = "true",
                                    MNTH = mnth,
                                    EMP_NO = "NOT FOUND",
                                    EMP_ID = "NOT FOUND",
                                    EMP_NAME = "NOT FOUND",
                                    PUNKTY = 0,
                                    PROG = 0,
                                    PREMIA = 0,
                                    CTRL_REK = null
                                };
                                main_rek = re;
                            }
                        }
                    }
                }
                instances = instances - 1;
                return main_rek;
            }
            catch (Exception e)
            {
                instances = instances - 1;
                throw new Exception("ExecuteQuery Error", e);
            }
        }
        public async Task<Ctr_rek> Person_control(string per_id, string mnth)
        {
            try
            {
                while (instances > 0)
                {
                    if (instances > 0) { System.Threading.Thread.Sleep(300); }
                }
                instances = instances + 1;
                var main_rek = new Ctr_rek();
                var rek_data = new List<Emp_ctrl>();
                string _EMP_NO = " ";
                string _EMP_ID = " ";
                string _EMP_NAME = " ";
                double numrek = 0;
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {

                    using (OracleCommand kal = new OracleCommand("select a.EMP_NO,ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO) EMP_ID,ifsapp.person_info_api.Get_Name(ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO)) EMP_Name,Decode(a.Q_C_STATUS_DB,'TRUE',1,DEcode(nvl(IFSAPP.C_SHOP_ORD_QPC_LN_API.Get_Sum_Factor( a.ID_QPC ),0),0 ,0,1,1,2,2,3,3,3)*-3) * a.QUANTITY Punkty_zlec,0 PUNKTY,a.ID_QPC ,a.ORDER_NO,ifsapp.shop_ord_api.Get_Part_No(a.ORDER_NO,a.RELEASE_NO,a.SEQUENCE_NO) part_no,ifsapp.inventory_part_api.Get_Description('ST',ifsapp.shop_ord_api.Get_Part_No(a.ORDER_NO,a.RELEASE_NO,a.SEQUENCE_NO)) descr,Q_C_DATE,a.QUANTITY,a.OPERATION_NO,a.CONTROLER_PERSON,ifsapp.person_info_api.Get_Name(a.CONTROLER_PERSON) Controler_name,a.Q_C_STATUS FROM IFSAPP.C_SHOP_ORD_QPC_HD a WHERE a.EMP_NO=:emp_no and to_char(a.Q_C_DATE,'YYYYMM')=:mnth Order by  a.Q_C_DATE,a.objversion", conO))
                    {
                        await conO.OpenAsync();
                        kal.Parameters.Add(new OracleParameter(":emp_no", OracleDbType.Varchar2) { Value = per_id });
                        kal.Parameters.Add(new OracleParameter(":mnth", OracleDbType.Varchar2) { Value = mnth });
                        using (var reader = await kal.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                        {
                            double points = 0;
                            double ctr_point = 0;
                            double prog = 0;
                            double premia = 0;
                            string klasa = "";
                            int i = 0;
                            while (await reader.ReadAsync())
                            {
                                numrek = numrek + 1;
                                if (i == 0)
                                {
                                    _EMP_NO = reader.GetString(0);
                                    _EMP_ID = reader.GetString(1);
                                    _EMP_NAME = reader.GetString(2);
                                    i = i + 1;
                                }
                                ctr_point = reader.GetDouble(3);
                                points = points + ctr_point;
                                if (points >= 50)
                                {
                                    points = 50;
                                    prog = 5;
                                    premia = 110;
                                    klasa = "success";
                                }
                                else if (points < -30)
                                {
                                    points = -30;
                                    prog = 0;
                                    premia = 0;
                                    klasa = "danger";
                                }
                                else if (points < 50 & points >= 31)
                                {
                                    prog = 4;
                                    premia = 105;
                                    klasa = "info";
                                }
                                else if (points < 31 & points >= 0)
                                {
                                    prog = 3;
                                    premia = 100;
                                    klasa = "active";
                                }
                                else if (points < 0 & points >= -10)
                                {
                                    prog = 2;
                                    premia = 80;
                                    klasa = " ";
                                }
                                else if (points < -10 & points > -30)
                                {
                                    prog = 1;
                                    premia = points * 2 + 100;
                                    klasa = "warning";
                                }
                                var rek = new Emp_ctrl
                                {
                                    NUM_REK = numrek,
                                    PUNKTY_zlec = reader.GetDouble(3),
                                    PUNKTY = points,
                                    PROG = prog,
                                    PREMIA = premia,
                                    Class = klasa,
                                    ID_QPC = reader.GetString(5),
                                    ORDER_NO = reader.GetString(6),
                                    PART_NO = reader.GetString(7),
                                    DESCR = reader.GetString(8),
                                    Q_C_DATE = reader.GetDateTime(9).ToString(),
                                    QUANTITY = reader.GetDouble(10),
                                    OPERATION_NO = reader.GetDouble(11),
                                    CONTROLER_PERSON = reader.GetString(12),
                                    CONTROLER_NAME = reader.GetString(13),
                                    Q_C_STATUS = reader.GetString(14)
                                };
                                rek_data.Add(rek);
                            }
                            if (i != 0)
                            {
                                var re = new Ctr_rek
                                {
                                    DATA_VALID = "true",
                                    MNTH = mnth,
                                    EMP_NO = _EMP_NO,
                                    EMP_ID = _EMP_ID,
                                    EMP_NAME = _EMP_NAME,
                                    PUNKTY = points,
                                    PROG = prog,
                                    PREMIA = premia,
                                    CTRL_REK = rek_data
                                };
                                main_rek = re;
                            }
                            else
                            {
                                var re = new Ctr_rek
                                {
                                    DATA_VALID = "true",
                                    MNTH = mnth,
                                    EMP_NO = "NOT FOUND",
                                    EMP_ID = "NOT FOUND",
                                    EMP_NAME = "NOT FOUND",
                                    PUNKTY = 0,
                                    PROG = 0,
                                    PREMIA = 0,
                                    CTRL_REK = null
                                };
                                main_rek = re;
                            }
                        }
                    }
                }
                instances = instances - 1;
                return main_rek;
            }
            catch (Exception e)
            {
                instances = instances - 1;
                throw new Exception("ExecuteQuery Error", e);
            }
        }
        public async Task<HDR_List_ctrl> List_control(string mnth)
        {
            try
            {
                while (instances > 0)
                {
                    if (instances > 0) { System.Threading.Thread.Sleep(300); }
                }
                instances = instances + 1;
                var main_rek = new List<List_ctrl>();
                var header = new HDR_List_ctrl();
                string _EMP_NO = " ";
                string _EMP_ID = " ";
                string _EMP_NAME = " ";
                double numrek = -1;
                double ec = 0;
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    using (OracleCommand kal = new OracleCommand("SELECT a.EMP_NO,a.EMP_ID,a.EMP_NAME,a.PUNKTY_ZLEC,Nvl(b.order_no,'brak') ORD_no FROM (select a.EMP_NO,ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO) EMP_ID,ifsapp.person_info_api.Get_Name(ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO)) EMP_Name,Decode(a.Q_C_STATUS_DB,'TRUE',1,DEcode(nvl(IFSAPP.C_SHOP_ORD_QPC_LN_API.Get_Sum_Factor( a.ID_QPC ),0),0 ,0,1,1,2,2,3,3,3)*-3) * a.QUANTITY Punkty_zlec,a.order_no FROM IFSAPP.C_SHOP_ORD_QPC_HD a WHERE EMP_NO IS NOT NULL and to_char(a.Q_C_DATE,'YYYYMM')=:mnth Order by  a.EMP_NO,a.Q_C_DATE,a.objversion) a left JOIN (SELECT a.order_no,b.emp_no FROM ifsapp.operation_history a,ifsapp.c_work_group_det b WHERE a.DATE_APPLIED>=To_Date(:mnth,'YYYYMM')-30 AND b.VALID_DATE>=To_Date(:mnth,'YYYYMM')-30 AND b.MEMBER_WORK_STATE_DB IN ('W','O') AND  b.WORK_GROUP_ID=Nvl(a.C_WORK_GROUP_ID,a.BRIGADE_NO) AND a.DATE_APPLIED=b.VALID_DATE GROUP BY a.order_no,b.emp_no) b ON b.order_no= a.order_no AND b.emp_no=a.EMP_NO", conO))
                    {
                        await conO.OpenAsync();
                        kal.Parameters.Add(new OracleParameter(":mnth", OracleDbType.Varchar2) { Value = mnth });
                        using (var reader = await kal.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                        {
                            double points = 0;
                            double ctr_point = 0;
                            double prog = 0;
                            double premia = 0;
                            string klasa = "";
                            int i = 0;
                            while (await reader.ReadAsync())
                            {
                                numrek = numrek + 1;
                                if (i == 0)
                                {
                                    _EMP_NO = reader.GetString(0);
                                    _EMP_ID = reader.GetString(1);
                                    _EMP_NAME = reader.GetString(2);
                                    i = i + 1;
                                }
                                else if (_EMP_NO != reader.GetString(0))
                                {
                                    var re = new List_ctrl
                                    {
                                        EMP_NO = _EMP_NO,
                                        EMP_ID = _EMP_ID,
                                        EMP_NAME = _EMP_NAME,
                                        PUNKTY = points,
                                        PROG = prog,
                                        PREMIA = premia,
                                        Class = klasa,
                                        L_Kontr = numrek,
                                        Nie_wyk = ec
                                    };
                                    main_rek.Add(re);
                                    numrek = 0;
                                    ec = 0;

                                    _EMP_NO = reader.GetString(0);
                                    _EMP_ID = reader.GetString(1);
                                    _EMP_NAME = reader.GetString(2);
                                    points = 0;
                                }
                                if (reader.GetString(4) == "brak")
                                {
                                    ec = ec + 1;
                                }
                                ctr_point = reader.GetDouble(3);
                                points = points + ctr_point;
                                if (points >= 50)
                                {
                                    points = 50;
                                    prog = 5;
                                    premia = 110;
                                    klasa = "success";
                                }
                                else if (points < -30)
                                {
                                    points = -30;
                                    prog = 0;
                                    premia = 0;
                                    klasa = "danger";
                                }
                                else if (points < 50 & points >= 31)
                                {
                                    prog = 4;
                                    premia = 105;
                                    klasa = "info";
                                }
                                else if (points < 31 & points >= 0)
                                {
                                    prog = 3;
                                    premia = 100;
                                    klasa = "active";
                                }
                                else if (points < 0 & points >= -10)
                                {
                                    prog = 2;
                                    premia = 80;
                                    klasa = " ";
                                }
                                else if (points < -10 & points > -30)
                                {
                                    prog = 1;
                                    premia = points * 2 + 100;
                                    klasa = "warning";
                                }
                            }
                            if (numrek != 0)
                            {
                                var re = new List_ctrl
                                {
                                    EMP_NO = _EMP_NO,
                                    EMP_ID = _EMP_ID,
                                    EMP_NAME = _EMP_NAME,
                                    PUNKTY = points,
                                    PROG = prog,
                                    PREMIA = premia,
                                    Class = klasa,
                                    L_Kontr = numrek,
                                    Nie_wyk = ec,
                                    VI = "0"
                                };
                                main_rek.Add(re);
                            }
                            else
                            {
                                var re = new List_ctrl
                                {
                                    EMP_NO = "NOT FOUND",
                                    EMP_ID = "NOT FOUND",
                                    EMP_NAME = "NOT FOUND",
                                    PUNKTY = 0,
                                    PROG = 0,
                                    PREMIA = 0,
                                    Class = "active",
                                    L_Kontr = 0,
                                    Nie_wyk = 0,
                                    VI = "0"
                                };
                                main_rek.Add(re);
                            }
                            var hdr = new HDR_List_ctrl
                            {
                                DATA_VALID = "true",
                                MNTH = mnth,
                                _Ctrls = main_rek
                            };
                            header = hdr;
                        }
                    }
                }
                instances = instances - 1;
                return header;
            }
            catch (Exception e)
            {
                instances = instances - 1;
                throw new Exception("ExecuteQuery Error", e);
            }
        }
        public async Task<HDR_Ctrl_detal> Control_detalis(string ctrl_id)
        {
            try
            {
                while (instances > 0)
                {
                    if (instances > 0) { System.Threading.Thread.Sleep(300); }
                }
                instances = instances + 1;
                var rek_data = new List<Ctrl_detalis>();
                var ret_hdr = new HDR_Ctrl_detal();
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {

                    using (OracleCommand kal = new OracleCommand("SELECT ID_QPC,LINE_NO,NCR_PROC_CODE,ifsapp.C_Ncr_Proc_Codes_Api.Get_Ncr_Proc_Desc(NCR_PROC_CODE) opis,ifsapp.C_Ncr_Proc_Codes_Api.Get_Ncr_Factor(NCR_PROC_CODE) punkty FROM ifsapp.C_SHOP_ORD_QPC_LN a  WHERE a.ID_QPC=:IDQPC ", conO))
                    {
                        await conO.OpenAsync();
                        kal.Parameters.Add(new OracleParameter(":IDQPC", OracleDbType.Varchar2) { Value = ctrl_id });
                        using (var reader = await kal.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                        {
                            int i = 0;
                            while (await reader.ReadAsync())
                            {
                                if (i == 0)
                                {
                                    i = 1;
                                }
                                var rek = new Ctrl_detalis
                                {
                                    LINE_NO = reader.GetString(1),
                                    NCR_PROC_CODE = reader.GetString(2),
                                    OPIS = reader.GetString(3),
                                    PUNKTY = reader.GetDouble(4)
                                };
                                rek_data.Add(rek);
                            }
                            if (i == 0)
                            {
                                var rek = new Ctrl_detalis
                                {
                                    LINE_NO = "NO DATA",
                                    NCR_PROC_CODE = "EMPTY",
                                    OPIS = "DANE ZOSTAŁY USUNIĘTE",
                                    PUNKTY = 0
                                };
                                rek_data.Add(rek);
                            }
                            var hdr = new HDR_Ctrl_detal
                            {
                                DATA_VALID = "true",
                                ID_QPC = ctrl_id,
                                DTA_detal = rek_data
                            };
                            ret_hdr = hdr;
                        }
                    }
                }
                instances = instances - 1;
                return ret_hdr;
            }
            catch (Exception e)
            {
                instances = instances - 1;
                throw new Exception("ExecuteQuery Error", e);
            }
        }
        public async Task<HDR_List_kontrolers> Kotrl_stat(string mnth)
        {
            try
            {
                while (instances > 0)
                {
                    if (instances > 0) { System.Threading.Thread.Sleep(300); }
                }
                instances = instances + 1;
                var rek_data = new List<List_kontrolers>();
                var header = new HDR_List_kontrolers();
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {

                    using (OracleCommand kal = new OracleCommand("SELECT a.*,Round(Nvl(b.BLAD_KONTROLI,0)/a.L_KONTR*100,2) Proc_bl_kontr,Nvl(b.BLAD_KONTROLI,0) L_blednych_kontr,Nvl(b.Nie_zatrudniony,0) Nie_zatrudniony,Nvl(b.Nie_wyk_zlec,0) Nie_wyk_zlec,Nvl(b.Niezg_bez_uwag,0) Niezg_bez_uwag FROM (SELECT a.CONTROLER_PERSON,ifsapp.person_info_api.Get_Name(a.CONTROLER_PERSON) Controler_name,Count(a.Q_C_STATUS) L_kontr,Sum(Decode(a.Q_C_STATUS,'Zgodny',1,0)) Sum_zgodn,sum(Decode(a.Q_C_STATUS,'Niezgodny',1,0)) Sum_niezgodn,b.l_dni,Round(Count(a.Q_C_STATUS)/b.l_dni) l_kontr_dzien FROM IFSAPP.C_SHOP_ORD_QPC_HD a left JOIN (SELECT CONTROLER_PERSON,Sum(liczba) l_dni FROM (SELECT cONTROLER_PERSON,To_Date(Q_C_DATE) dat,1 liczba FROM IFSAPP.C_SHOP_ORD_QPC_HD WHERE to_char(Q_C_DATE,'YYYYMM')=:mnth GROUP BY cONTROLER_PERSON,To_Date(Q_C_DATE)) GROUP BY CONTROLER_PERSON) b ON b.cONTROLER_PERSON=a.CONTROLER_PERSON WHERE to_char(a.Q_C_DATE,'YYYYMM')=:mnth GROUP BY a.CONTROLER_PERSON,b.l_dni) a left JOIN (SELECT CONTROLER_PERSON,Count(uwagi) blad_kontroli, Sum(Decode(uwagi,'1',1,0)) Nie_zatrudniony,Sum(Decode(uwagi,'2',1,0)) Nie_wyk_zlec,Sum(Decode(uwagi,'3',1,0)) Niezg_bez_uwag FROM (SELECT Decode(b.emp_no,NULL,Decode(c.last_work,NULL,'1','2'),'3') Uwagi,a.EMP_NO,a.CONTROLER_PERSON,a.Q_C_STATUS,b.emp_no,c.Last_work FROM IFSAPP.C_SHOP_ORD_QPC_HD a left JOIN (SELECT a.order_no,b.emp_no FROM ifsapp.operation_history a,ifsapp.c_work_group_det b WHERE a.DATE_APPLIED>=To_Date(:mnth,'YYYYMM')-30 AND b.VALID_DATE>=To_Date(:mnth,'YYYYMM')-30  AND b.MEMBER_WORK_STATE_DB IN ('W','O') AND  b.WORK_GROUP_ID=Nvl(a.C_WORK_GROUP_ID,a.BRIGADE_NO) AND a.DATE_APPLIED=b.VALID_DATE) b ON b.order_no= a.order_no AND b.emp_no=a.EMP_NO left JOIN (SELECT emp_no,Max(VALID_DATE) last_work FROM ifsapp.c_work_group_det WHERE vALID_DATE>=To_Date(:mnth,'YYYYMM')-180 AND MEMBER_WORK_STATE_DB IN ('W','O') GROUP BY emp_no) c ON c.emp_no=a.emp_no WHERE (b.emp_no IS NULL OR (Q_C_STATUS='Niezgodny' AND DEcode(nvl(IFSAPP.C_SHOP_ORD_QPC_LN_API.Get_Sum_Factor( a.ID_QPC ),0),0 ,0,1,1,2,2,3,3,3)=0)) AND to_char(a.Q_C_DATE,'YYYYMM')=:mnth AND a.emp_no IS NOT NULL ) GROUP BY  CONTROLER_PERSON) b ON  b.CONTROLER_PERSON=a.CONTROLER_PERSON ORDER BY a.CONTROLER_PERSON ", conO))
                    {
                        await conO.OpenAsync();
                        kal.Parameters.Add(new OracleParameter(":mnth", OracleDbType.Varchar2) { Value = mnth });
                        using (var reader = await kal.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                        {
                            int i = 0;
                            while (await reader.ReadAsync())
                            {
                                if (i == 0)
                                {
                                    i = 1;
                                }
                                var rek = new List_kontrolers
                                {
                                    CNTR_PER = reader.GetString(0),
                                    CNTR_NAME = reader.GetString(1),
                                    L_KONTR = reader.GetDouble(2),
                                    SUM_ZGODN = reader.GetDouble(3),
                                    SUM_NIEZG = reader.GetDouble(4),
                                    L_DNI = reader.GetDouble(5),
                                    L_KON_DZIE = reader.GetDouble(6),
                                    PROC_BL_KONTR = reader.GetDouble(7),
                                    L_BLEDNYCH_KONTR = reader.GetDouble(8),
                                    NIE_ZATRUDNIONY = reader.GetDouble(9),
                                    NIE_WYK_ZLEC = reader.GetDouble(10),
                                    NIEZG_BEZ_UWAG = reader.GetDouble(11),
                                    VI = "0"
                                };
                                rek_data.Add(rek);
                            }
                            if (i == 0)
                            {
                                var rek = new List_kontrolers
                                {
                                    CNTR_PER = "NO DATA",
                                    CNTR_NAME = "NO DATA",
                                    L_KONTR = 0,
                                    SUM_ZGODN = 0,
                                    SUM_NIEZG = 0,
                                    L_DNI = 0,
                                    L_KON_DZIE = 0,
                                    PROC_BL_KONTR = 0,
                                    L_BLEDNYCH_KONTR = 0,
                                    NIE_ZATRUDNIONY = 0,
                                    NIE_WYK_ZLEC = 0,
                                    NIEZG_BEZ_UWAG = 0,
                                    VI = "0"
                                };
                                rek_data.Add(rek);
                            }
                            var hdr = new HDR_List_kontrolers
                            {
                                DATA_VALID = "true",
                                MNTH = mnth,
                                _Ctrls = rek_data
                            };
                            header = hdr;
                        }
                    }
                }
                instances = instances - 1;
                return header;
            }
            catch (Exception e)
            {
                instances = instances - 1;
                throw new Exception("ExecuteQuery Error", e);
            }
        }
        public async Task<HDR_List_kontr_err> Kotrl_ERR_lst(string mnth, string kontr)
        {
            try
            {
                while (instances > 0)
                {
                    if (instances > 0) { System.Threading.Thread.Sleep(300); }
                }
                instances = instances + 1;
                var rek_data = new List<Kontrol_err>();
                var header = new HDR_List_kontr_err();
                string per_id = "";
                string fulname = "";
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    using (OracleCommand kal = new OracleCommand("SELECT Decode(b.emp_no,NULL,Decode(c.last_work,NULL,'Kontrola na osobę nie pracującą','Osoba nie wykonywała tego zlecenia'),'Wprowadzono niezgodność bez uwag') Uwagi,a.EMP_NO,ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO) EMP_ID,ifsapp.person_info_api.Get_Name(ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO)) EMP_Name,Decode(a.Q_C_STATUS_DB,'TRUE',1,DEcode(nvl(IFSAPP.C_SHOP_ORD_QPC_LN_API.Get_Sum_Factor( a.ID_QPC ),0),0 ,0,1,1,2,2,3,3,3)*-3) * a.QUANTITY Punkty_zlec,a.ID_QPC ,a.ORDER_NO,ifsapp.shop_ord_api.Get_Part_No(a.ORDER_NO,a.RELEASE_NO,a.SEQUENCE_NO) part_no,ifsapp.inventory_part_api.Get_Description('ST',ifsapp.shop_ord_api.Get_Part_No(a.ORDER_NO,a.RELEASE_NO,a.SEQUENCE_NO)) descr,Q_C_DATE,a.QUANTITY,a.CONTROLER_PERSON,ifsapp.person_info_api.Get_Name(a.CONTROLER_PERSON) Controler_name,a.Q_C_STATUS FROM IFSAPP.C_SHOP_ORD_QPC_HD a left JOIN (SELECT a.order_no,b.emp_no FROM ifsapp.operation_history a,ifsapp.c_work_group_det b  WHERE a.DATE_APPLIED>=To_Date(:mnth,'YYYYMM')-30 AND b.VALID_DATE>=To_Date(:mnth,'YYYYMM')-30  AND b.MEMBER_WORK_STATE_DB IN ('W','O') AND  b.WORK_GROUP_ID=Nvl(a.C_WORK_GROUP_ID,a.BRIGADE_NO) AND a.DATE_APPLIED=b.VALID_DATE) b ON b.order_no= a.order_no AND b.emp_no=a.EMP_NO left JOIN (SELECT emp_no,Max(VALID_DATE) last_work FROM ifsapp.c_work_group_det WHERE vALID_DATE>=To_Date(:mnth,'YYYYMM')-180 AND MEMBER_WORK_STATE_DB IN ('W','O') GROUP BY emp_no) c ON c.emp_no=a.emp_no  WHERE (b.emp_no IS NULL OR (Q_C_STATUS='Niezgodny' AND DEcode(nvl(IFSAPP.C_SHOP_ORD_QPC_LN_API.Get_Sum_Factor( a.ID_QPC ),0),0 ,0,1,1,2,2,3,3,3)=0))AND a.emp_no IS NOT null  AND to_char(a.Q_C_DATE,'YYYYMM')=:mnth AND a.CONTROLER_PERSON=:contr Order by  a.EMP_NO,a.Q_C_DATE,a.objversion", conO))
                    {
                        await conO.OpenAsync();
                        kal.BindByName = true;
                        kal.Parameters.Add(new OracleParameter(":mnth", OracleDbType.Varchar2) { Value = mnth });
                        kal.Parameters.Add(new OracleParameter(":contr", OracleDbType.Varchar2) { Value = kontr });
                        using (var reader = await kal.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                        {
                            int i = 0;
                            while (await reader.ReadAsync())
                            {
                                if (i == 0)
                                {
                                    per_id = reader.GetString(11);
                                    fulname = reader.GetString(12);
                                    i = 1;
                                }
                                var rek = new Kontrol_err
                                {
                                    UWAGI = reader.GetString(0),
                                    EMP_NO = reader.GetString(1),
                                    EMP_ID = reader.GetString(2),
                                    EMP_NAME = reader.GetString(3),
                                    PUNKTY_ZLEC = reader.GetDouble(4),
                                    ID_QPC = reader.GetString(5),
                                    ORDER_NO = reader.GetString(6),
                                    PART_NO = reader.GetString(7),
                                    DESCR = reader.GetString(8),
                                    Q_C_DATE = reader.GetDateTime(9).ToString(),
                                    QUANTITY = reader.GetDouble(10),
                                    Q_C_STATUS = reader.GetString(13)
                                };
                                rek_data.Add(rek);
                            }
                            if (i == 0)
                            {
                                var rek = new Kontrol_err
                                {
                                    UWAGI = "NO DATA",
                                    EMP_NAME = "NO DATA",
                                    EMP_NO = "NO DATA",
                                    EMP_ID = "NO DATA",
                                    PUNKTY_ZLEC = 0,
                                    ID_QPC = "NO DATA",
                                    ORDER_NO = "NO DATA",
                                    PART_NO = "NO DATA",
                                    DESCR = "NO DATA"
                                };
                                rek_data.Add(rek);
                            }
                            var hdr = new HDR_List_kontr_err
                            {
                                DATA_VALID = "true",
                                MNTH = mnth,
                                CNTR_PER = per_id,
                                CONTROLER_NAME = fulname,
                                _Ctrls = rek_data
                            };
                            header = hdr;
                        }
                    }
                }
                instances = instances - 1;
                return header;
            }
            catch (Exception e)
            {
                instances = instances - 1;
                throw new Exception("ExecuteQuery Error", e);
            }
        }
        public async Task<HDR_Ctrl_statistic> Rows_Ctrl_stat(string mnth, string emp_no)
        {
            try
            {
                while (instances > 0)
                {
                    if (instances > 0) { System.Threading.Thread.Sleep(300); }
                }
                instances = instances + 1;
                var rek_data = new List<Ctrl_statistic>();
                var header = new HDR_Ctrl_statistic();
                string per_id = "";
                string fulname = "";
                string grup = "";
                string shift = "";
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    using (OracleCommand kal = new OracleCommand("SELECT c.wydz grupy,a.shift,To_Char(a.VALID_DATE,'YY-mm-dd')VALID_DATE,a.EMP_NO,a.PERSON_ID,ifsapp.person_info_api.Get_Name(ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO)) EMP_Name,a.HOURS_wrk,Nvl(b.L_kontr,0) L_kont,Decode(Sign(Round((HOURS_WRK/4)-Nvl(b.L_kontr,0))),'1',Round((HOURS_WRK/4)-Nvl(b.L_kontr,0)),0) Brak_kontr FROM (SELECT Decode(SubStr(WORK_GROUP_ID,1,2),'22',SubStr(ifsapp.c_work_group_head_api.Get_Work_Group_Desc('ST',WORK_GROUP_ID),1,1),SubStr(WORK_GROUP_ID,-1,1)) shift,VALID_DATE,EMP_NO,PERSON_ID,Sum(HOURS_QTY) HOURS_wrk FROM ifsapp.c_work_group_det WHERE SubStr(WORK_GROUP_ID,1,1) NOT IN ('U','9','0') and To_Char(VALID_DATE,'YYYYMM')=:mnth AND EMP_NO=:emp_no AND MEMBER_WORK_STATE_DB IN ('W','O') AND HOURS_QTY>1 GROUP BY Decode(SubStr(WORK_GROUP_ID,1,2),'22',SubStr(ifsapp.c_work_group_head_api.Get_Work_Group_Desc('ST',WORK_GROUP_ID),1,1),SubStr(WORK_GROUP_ID,-1,1)),VALID_DATE,EMP_NO,PERSON_ID) a left JOIN (SELECT To_Date(Q_C_DATE) dat,EMP_NO,Count(EMP_NO) L_kontr FROM IFSAPP.C_SHOP_ORD_QPC_HD WHERE To_Char(Q_C_DATE,'YYYYMM')=:mnth AND EMP_NO=:emp_no GROUP BY To_Date(Q_C_DATE),EMP_NO) b ON b.EMP_NO||'_'||b.dat=a.EMP_NO||'_'||a.VALID_DATE left JOIN (SELECT emp_no,listagg(wydz,'; ') WITHIN GROUP (ORDER BY wydz) AS wydz from (SELECT a.emp_no,SubStr(WORK_GROUP_ID,1,5) wydz FROM ifsapp.c_work_group_det a,(SELECT emp_no,Max(VALID_DATE) id FROM ifsapp.c_work_group_det WHERE EMP_NO=:emp_no AND SubStr(WORK_GROUP_ID,1,1) NOT IN ('U','9','0') AND MEMBER_WORK_STATE_DB IN ('W','O') GROUP BY emp_no) b WHERE a.emp_no=b.emp_no AND a.VALID_DATE=b.id  GROUP BY a.emp_no,SubStr(WORK_GROUP_ID,1,5) ) GROUP BY emp_no)c ON c.emp_no=a.emp_no ORDER BY EMP_NO,VALID_DATE", conO))
                    {
                        await conO.OpenAsync();
                        kal.BindByName = true;
                        kal.Parameters.Add(new OracleParameter(":mnth", OracleDbType.Varchar2) { Value = mnth });
                        kal.Parameters.Add(new OracleParameter(":emp_no", OracleDbType.Varchar2) { Value = emp_no });
                        using (var reader = await kal.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                        {
                            int i = 0;
                            while (await reader.ReadAsync())
                            {
                                if (i == 0)
                                {
                                    per_id = reader.GetString(4);
                                    fulname = reader.GetString(5);
                                    grup = reader.GetString(0);
                                    shift = reader.GetString(1);

                                    i = 1;
                                }
                                var rek = new Ctrl_statistic
                                {
                                    VALID_DATE = reader.GetString(2),
                                    GROUP = reader.GetString(0),
                                    SHIFT = reader.GetString(1),
                                    HOURS_WRK = reader.GetDouble(6),
                                    L_KONT = reader.GetDouble(7),
                                    BRAK_KONTR = reader.GetDouble(8),

                                };
                                rek_data.Add(rek);
                            }
                            if (i == 0)
                            {
                                var rek = new Ctrl_statistic
                                {
                                    VALID_DATE = "null",
                                    HOURS_WRK = 0,
                                    L_KONT = 0,
                                    BRAK_KONTR = 0,
                                };
                                rek_data.Add(rek);
                            }
                            var hdr = new HDR_Ctrl_statistic
                            {

                                DATA_VALID = "true",
                                MNTH = mnth,
                                EMP_NO = emp_no,
                                PER_ID = per_id,
                                EMP_NAME = fulname,
                                GRUPY = grup,
                                SHIFT = shift,
                                _Ctrls = rek_data
                            };
                            header = hdr;
                        }
                    }
                }
                instances = instances - 1;
                return header;
            }
            catch (Exception e)
            {
                instances = instances - 1;
                throw new Exception("ExecuteQuery Error", e);
            }
        }
        public async Task<HDR_ct_lst> Emp_ctrl_summ(string mnth)
        {
            try
            {
                while (instances > 0)
                {
                    if (instances > 0) { System.Threading.Thread.Sleep(300); }
                }
                instances = instances + 1;
                var rek_data = new List<Full_Ctrl_statistic>();
                var header = new HDR_ct_lst();
                var days = new int[32];
                int lst_day = 0;
                for (int i = 0; i < 32; i++)
                {
                    days[i] = 0;
                }
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    using (OracleCommand kal = new OracleCommand("SELECT c.wydz grupy,a.shift,To_Char(a.VALID_DATE,'dd')VALID_DATE,a.EMP_NO,a.PERSON_ID,ifsapp.person_info_api.Get_Name(ifsapp.company_person_api.Get_Person_Id('SITS',a.EMP_NO)) EMP_Name,a.HOURS_wrk,Nvl(b.L_kontr,0) L_kont,Decode(Sign(Round((HOURS_WRK/4)-Nvl(b.L_kontr,0))),'1',Round((HOURS_WRK/4)-Nvl(b.L_kontr,0)),0) Brak_kontr FROM (SELECT listagg(shift,'; ') within GROUP (ORDER BY shift) shift,VALID_DATE,EMP_NO,PERSON_ID,Sum(HOURS_wrk) HOURS_wrk FROM (SELECT Decode(SubStr(WORK_GROUP_ID,1,2),'22',SubStr(ifsapp.c_work_group_head_api.Get_Work_Group_Desc('ST',WORK_GROUP_ID),1,1),SubStr(WORK_GROUP_ID,-1,1)) shift,VALID_DATE,EMP_NO,PERSON_ID,Sum(HOURS_QTY) HOURS_wrk FROM ifsapp.c_work_group_det WHERE SubStr(WORK_GROUP_ID,1,1) NOT IN ('U','9','0') and To_Char(VALID_DATE,'YYYYMM')=:mnth AND  VALID_DATE<=SYSDATE AND MEMBER_WORK_STATE_DB IN ('W','O') AND HOURS_QTY>1 GROUP BY Decode(SubStr(WORK_GROUP_ID,1,2),'22',SubStr(ifsapp.c_work_group_head_api.Get_Work_Group_Desc('ST',WORK_GROUP_ID),1,1),SubStr(WORK_GROUP_ID,-1,1)), VALID_DATE,EMP_NO,PERSON_ID)GROUP BY VALID_DATE,EMP_NO,PERSON_ID) a left JOIN (SELECT To_Date(Q_C_DATE) dat,EMP_NO,Count(EMP_NO) L_kontr FROM IFSAPP.C_SHOP_ORD_QPC_HD WHERE To_Char(Q_C_DATE,'YYYYMM')=:mnth GROUP BY To_Date(Q_C_DATE),EMP_NO) b ON b.EMP_NO||'_'||b.dat=a.EMP_NO||'_'||a.VALID_DATE left JOIN (SELECT emp_no,listagg(wydz,'; ') WITHIN GROUP (ORDER BY wydz) AS wydz from (SELECT a.emp_no,SubStr(WORK_GROUP_ID,1,5) wydz FROM ifsapp.c_work_group_det a,(SELECT emp_no,Max(VALID_DATE) id FROM ifsapp.c_work_group_det WHERE  VALID_DATE<=SYSDATE AND SubStr(WORK_GROUP_ID,1,1) NOT IN ('U','9','0') AND MEMBER_WORK_STATE_DB IN ('W','O') GROUP BY emp_no) b WHERE a.emp_no=b.emp_no AND a.VALID_DATE=b.id  GROUP BY a.emp_no,SubStr(WORK_GROUP_ID,1,5) ) GROUP BY emp_no)c ON c.emp_no=a.emp_no ORDER BY EMP_NO,PERSON_ID,VALID_DATE", conO))
                    {
                        await conO.OpenAsync();
                        kal.BindByName = true;
                        kal.Parameters.Add(new OracleParameter(":mnth", OracleDbType.Varchar2) { Value = mnth });
                        using (var reader = await kal.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                        {
                            while (await reader.ReadAsync())
                            {
                                if (days[Convert.ToInt32(reader.GetString(2).Substring(reader.GetString(2).Length - 2))] == 0)
                                {
                                    if (lst_day < Convert.ToInt32(reader.GetString(2).Substring(reader.GetString(2).Length - 2)))
                                    {
                                        lst_day = Convert.ToInt32(reader.GetString(2).Substring(reader.GetString(2).Length - 2));
                                    }
                                    days[Convert.ToInt32(reader.GetString(2).Substring(reader.GetString(2).Length - 2))] = 1;
                                }
                                var rek = new Full_Ctrl_statistic
                                {
                                    GRUPY = reader.GetString(0),
                                    SHIFT = reader.GetString(1),
                                    VALID_DATE = reader.GetString(2),
                                    EMP_NO = reader.GetString(3),
                                    PER_ID = reader.GetString(4),
                                    EMP_NAME = reader.GetString(5),
                                    HOURS_WRK = reader.GetDouble(6),
                                    L_KONT = reader.GetDouble(7),
                                    BRAK_KONTR = reader.GetDouble(8)
                                };
                                rek_data.Add(rek);
                            }
                        }
                    }
                }
                instances = instances - 1;
                List<object> rows = new List<object>();
                var dataObject = new ExpandoObject() as IDictionary<string, Object>;
                string emp_no = "";
                string pers_id = "";
                int last_day = 0;
                try
                {
                    int L_rek = rek_data.Count;
                    foreach (Full_Ctrl_statistic rek in rek_data)
                    {
                        L_rek = L_rek - 1;
                        if (rek.EMP_NO != emp_no || rek.PER_ID != pers_id)
                        {
                            if (emp_no != "")
                            {
                                // sprawdź czy may wprowadzone dni do końca
                                if (lst_day > last_day)
                                {
                                    for (int i = last_day + 1; i <= lst_day; i++)
                                    {
                                        if (days[i] != 0)
                                        {
                                            var daq = i.ToString();
                                            if (i.ToString().Length == 1)
                                            {
                                                daq = "0" + daq;
                                            }
                                            string v_dat = daq;
                                            dataObject.Add("_" + v_dat, "");
                                        }
                                    }
                                }
                                rows.Add(dataObject);
                                dataObject = new ExpandoObject() as IDictionary<string, Object>;
                            }
                            emp_no = rek.EMP_NO;
                            pers_id = rek.PER_ID;
                            dataObject.Add("emp_no", rek.EMP_NO);
                            dataObject.Add("pers_id", rek.PER_ID);
                            dataObject.Add("emp_name", rek.EMP_NAME);
                            dataObject.Add("shift", rek.SHIFT);
                            dataObject.Add("grupy", rek.GRUPY);
                            dataObject.Add("vi", "0");
                            last_day = 0;
                        }
                        if (last_day + 1 == Convert.ToInt32(rek.VALID_DATE.Substring(rek.VALID_DATE.Length - 2)))
                        {
                            // kolejny dzień dodajemy
                            string inf = rek.HOURS_WRK + "h/" + rek.L_KONT + "kont.<br/>";
                            if (rek.BRAK_KONTR == 0)
                            {
                                inf = "OK.";
                            }
                            else
                            {
                                inf = inf + "Brak " + rek.BRAK_KONTR + " kontr.";
                            }
                            dataObject.Add("_" + rek.VALID_DATE, inf);
                        }
                        else
                        {

                            for (int i = last_day + 1; i < Convert.ToInt32(rek.VALID_DATE.Substring(rek.VALID_DATE.Length - 2)); i++)
                            {
                                if (days[i] != 0)
                                {
                                    var daq = i.ToString();
                                    if (i.ToString().Length == 1)
                                    {
                                        daq = "0" + daq;
                                    }
                                    string v_dat = daq;
                                    dataObject.Add("_" + v_dat, "");
                                }
                            }
                            string inf = rek.HOURS_WRK + "h/" + rek.L_KONT + "kont.<br/>";
                            if (rek.BRAK_KONTR == 0)
                            {
                                inf = "OK.";
                            }
                            else
                            {
                                inf = inf + "Brak " + rek.BRAK_KONTR + " kontr.";
                            }
                            dataObject.Add("_" + rek.VALID_DATE, inf);
                        }
                        last_day = Convert.ToInt32(rek.VALID_DATE.Substring(rek.VALID_DATE.Length - 2));
                        if (L_rek == 0)
                        {
                            if (emp_no != "")
                            {
                                // sprawdź czy may wprowadzone dni do końca
                                if (lst_day > last_day)
                                {
                                    for (int i = last_day + 1; i <= lst_day; i++)
                                    {
                                        if (days[i] != 0)
                                        {
                                            var daq = i.ToString();
                                            if (i.ToString().Length == 1)
                                            {
                                                daq = "0" + daq;
                                            }
                                            string v_dat = daq;
                                            dataObject.Add("_" + v_dat, "");
                                        }
                                    }
                                }
                                rows.Add(dataObject);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    instances = instances - 1;
                    throw new Exception("ExecuteQuery Error", e);
                }
                var head = new HDR_ct_lst
                {
                    DATA_VALID = "true",
                    MNTH = mnth,
                    rkds = rows
                };
                header = head;
                return header;
            }
            catch (Exception e)
            {
                instances = instances - 1;
                throw new Exception("ExecuteQuery Error", e);
            }
        }
        public class Ctr_rek
        {
            public string DATA_VALID { get; set; }
            public string MNTH { get; set; }
            public string EMP_NO { get; set; }
            public string EMP_ID { get; set; }
            public string EMP_NAME { get; set; }
            public double PUNKTY { get; set; }
            public double PROG { get; set; }
            public double PREMIA { get; set; }
            public List<Emp_ctrl> CTRL_REK { get; set; }
        }
        public class Ctr_rek_E
        {
            public string DATA_VALID { get; set; }
            public string MNTH { get; set; }
            public string EMP_NO { get; set; }
            public string EMP_ID { get; set; }
            public string EMP_NAME { get; set; }
            public double PUNKTY { get; set; }
            public double PROG { get; set; }
            public double PREMIA { get; set; }
            public List<Emp_ctrl_E> CTRL_REK { get; set; }
        }
        public class Emp_ctrl
        {
            public double NUM_REK { get; set; }
            public string Q_C_DATE { get; set; }
            public string Class { get; set; }
            public string ORDER_NO { get; set; }
            public string PART_NO { get; set; }
            public string DESCR { get; set; }
            public double QUANTITY { get; set; }
            public double PUNKTY_zlec { get; set; }
            public double PUNKTY { get; set; }
            public double PROG { get; set; }
            public double PREMIA { get; set; }
            public double OPERATION_NO { get; set; }
            public string CONTROLER_PERSON { get; set; }
            public string CONTROLER_NAME { get; set; }
            public string Q_C_STATUS { get; set; }
            public string ID_QPC { get; set; }
        }
        public class Emp_ctrl_E
        {
            public double NUM_REK { get; set; }
            public string Q_C_DATE { get; set; }
            public string VALID { get; set; }
            public string ORDER_NO { get; set; }
            public string PART_NO { get; set; }
            public string DESCR { get; set; }
            public double QUANTITY { get; set; }
            public double PUNKTY_zlec { get; set; }
            public string CNTR_PERSON { get; set; }
            public string CNTR_NAME { get; set; }
            public string Q_C_STATUS { get; set; }
        }
        public class Ctrl_detalis
        {
            public string LINE_NO { get; set; }
            public string NCR_PROC_CODE { get; set; }
            public string OPIS { get; set; }
            public double PUNKTY { get; set; }
        }
        public class HDR_Ctrl_detal
        {
            public string DATA_VALID { get; set; }
            public string ID_QPC { get; set; }
            public List<Ctrl_detalis> DTA_detal { get; set; }
        }
        public class HDR_List_ctrl
        {
            public string DATA_VALID { get; set; }
            public string MNTH { get; set; }
            public List<List_ctrl> _Ctrls { get; set; }
        }
        public class List_ctrl
        {
            public string EMP_NO { get; set; }
            public string EMP_ID { get; set; }
            public string EMP_NAME { get; set; }
            public double PUNKTY { get; set; }
            public double PROG { get; set; }
            public double PREMIA { get; set; }
            public string Class { get; set; }
            public double L_Kontr { get; set; }
            public double Nie_wyk { get; set; }
            public string VI { get; set; }
        }
        public class List_kontrolers
        {
            public string CNTR_PER { get; set; }
            public string CNTR_NAME { get; set; }
            public double L_KONTR { get; set; }
            public double SUM_ZGODN { get; set; }
            public double SUM_NIEZG { get; set; }
            public double L_DNI { get; set; }
            public double L_KON_DZIE { get; set; }
            public double PROC_BL_KONTR { get; set; }
            public double L_BLEDNYCH_KONTR { get; set; }
            public double NIE_ZATRUDNIONY { get; set; }
            public double NIE_WYK_ZLEC { get; set; }
            public double NIEZG_BEZ_UWAG { get; set; }
            public string VI { get; set; }
        }
        public class HDR_List_kontrolers
        {
            public string DATA_VALID { get; set; }
            public string MNTH { get; set; }
            public List<List_kontrolers> _Ctrls { get; set; }
        }
        public class Kontrol_err
        {
            public string UWAGI { get; set; }
            public string EMP_NO { get; set; }
            public string EMP_ID { get; set; }
            public string EMP_NAME { get; set; }
            public double PUNKTY_ZLEC { get; set; }
            public string ID_QPC { get; set; }
            public string ORDER_NO { get; set; }
            public string PART_NO { get; set; }
            public string DESCR { get; set; }
            public string Q_C_DATE { get; set; }
            public double QUANTITY { get; set; }
            public string Q_C_STATUS { get; set; }
        }
        public class HDR_List_kontr_err
        {
            public string DATA_VALID { get; set; }
            public string MNTH { get; set; }
            public string CNTR_PER { get; set; }
            public string CONTROLER_NAME { get; set; }
            public List<Kontrol_err> _Ctrls { get; set; }
        }
        public class Ctrl_statistic
        {
            public string VALID_DATE { get; set; }
            public string GROUP { get; set; }
            public string SHIFT { get; set; }
            public double HOURS_WRK { get; set; }
            public double L_KONT { get; set; }
            public double BRAK_KONTR { get; set; }
        }
        public class HDR_Ctrl_statistic
        {
            public string DATA_VALID { get; set; }
            public string MNTH { get; set; }
            public string EMP_NO { get; set; }
            public string PER_ID { get; set; }
            public string EMP_NAME { get; set; }
            public string GRUPY { get; set; }
            public string SHIFT { get; set; }
            public List<Ctrl_statistic> _Ctrls { get; set; }
        }
        public class Full_Ctrl_statistic
        {
            public string EMP_NO { get; set; }
            public string PER_ID { get; set; }
            public string EMP_NAME { get; set; }
            public string GRUPY { get; set; }
            public string SHIFT { get; set; }
            public string VALID_DATE { get; set; }
            public double HOURS_WRK { get; set; }
            public double L_KONT { get; set; }
            public double BRAK_KONTR { get; set; }
        }
        public class HDR_ct_lst
        {
            public string DATA_VALID { get; set; }
            public string MNTH { get; set; }
            public List<object> rkds { get; set; }
        }
    }
}

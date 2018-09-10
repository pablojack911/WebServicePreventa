using System;
using System.Text;
using System.Web;
using System.Web.Services;
using System.Web.Script.Services;
using System.Web.Services.Protocols;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.IO;
using System.Collections;
using System.Drawing;
using System.IO.Compression;

[WebService(Namespace = "http://www.inteldevmobile.com.ar")]
[WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
public class Service : System.Web.Services.WebService
{
    int distribuidora;
    int tiempotimeout = 3600;
    string stringConnection = System.Configuration.ConfigurationManager.ConnectionStrings["idevdbmobile"].ToString();

    public Service()
    {
        //Uncomment the following line if using designed components
        //InitializeComponent();
    }

    [WebMethod]
    public string validarConexion(string usuario, string clave, string imei)
    {
        try
        {
            using (SqlConnection con = new SqlConnection(stringConnection))
            {
                SqlCommand cmdSql = con.CreateCommand();
                cmdSql.CommandType = CommandType.Text;
                cmdSql.CommandText = "select * from sysimeis where imei=@imei and borrado<>1 and suspendido<>1";
                cmdSql.Parameters.AddWithValue("@imei", imei);
                con.Open();
                using (SqlDataReader drImei = cmdSql.ExecuteReader())
                {
                    if (drImei.HasRows)
                    {
                        DataTable dtImei = new DataTable();
                        dtImei.Load(drImei);
                        distribuidora = Convert.ToInt32(dtImei.Rows[0]["iddistribuidora"].ToString());
                        return "";
                    }
                    else
                    {
                        pendientes(imei, usuario);
                        return "Equipo inhabilitado - " + imei;
                    }
                }
            }
        }
        catch (Exception e)
        {
            return e.Message;
        }
    }

    

    [WebMethod]
    public string bajarDatosCliente(string usuario, string clave, string imei)
    {
        string respuesta;
        string xmlSalida = "";
        try
        {
            respuesta = validarConexion(usuario, clave, imei);
            if (respuesta.Length != 0)
            {
                return respuesta;
            }
        }
        catch (Exception ex)
        {
            return "validarConeccion en bajarDatosCliente - " + ex.Message;
        }
        try
        {
            ArrayList aTablas = new ArrayList();
            aTablas.Add("caboper");
            aTablas.Add("detoper");
            xmlSalida = pidedatos2(distribuidora, "Pa_BajadaCliente", imei, aTablas);
        }
        catch (Exception ex)
        {
            return "pidedatos2 en bajarDatosCliente - Error al bajar los datos - " + ex.Message;
        }
        return xmlSalida;
    }

    [WebMethod]
    public string subirDatosCliente(string usuario, string clave, string imei, string xmlDatos)
    {
        string respuesta;
        DataSet ds;
        try
        {
            respuesta = validarConexion(usuario, clave, imei);
            if (respuesta.Length != 0)
            {
                return respuesta;
            }
        }
        catch (Exception ex)
        {
            return "validarConeccion en subirDatosCliente - " + ex.Message;
        }
        try
        {
            ds = CargaXmlaDataset(xmlDatos);
        }
        catch (Exception ex)
        {
            return "CargaXmlaDataset en subirDatosCliente - Formato de xml no valido - " + ex.Message;
        }
        string xml = PasaDtAxml(ds.Tables["Rubros"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActRubros");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Rubros: " + e.Message;
        }
        xml = PasaDtAxml(ds.Tables["Lineas"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActLineas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Lineas:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["Condvtas"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActCondVtas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Condevtas:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["Zonas"]);
        try
        {
            actualizaTabla(xml, 1, "PA_ActZonas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Zonas:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["Vendedores"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActVendedores");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Vendedores:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["Articulos"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActArticulos");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Articulos:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["CabListas"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActCabListas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla CabListas:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["DetListas"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActDetListas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla DetListas:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["RutaVentas"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActRutaVtas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla RutaVtas:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["ConfigRutas"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActConfigRutas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla ConfigRuta:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["Ramos"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActRamos");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Ramos:" + e.Message; ;
        }
        xml = PasaDtAxml(ds.Tables["Clientes"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActClientes");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Clientes:" + e.Message; ;
        }
        return "";
    }

    [WebMethod]
    public string subirNewConvenios(string usuario, string clave, string imei, string xmlDatos)
    {
        string respuesta;
        string xml;
        DataSet ds;
        try
        {
            respuesta = validarConexion(usuario, clave, imei);
            if (respuesta.Length != 0)
            {
                return respuesta;
            }
        }
        catch (Exception ex)
        {
            return "validarConeccion en subirNewConvenios - " + ex.Message;
        }
        try
        {
            ds = CargaXmlaDataset(xmlDatos);
        }
        catch
        {
            return "Formato de xml no valido";
        }
        try
        {
            if (ds.Tables.Contains("cabBonif") == true)
            {
                xml = PasaDtAxml(ds.Tables["CabBonif"]);
                actualizaTabla(xml, distribuidora, "PA_NewCabBonif");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Cabecera de Bonificaciones:" + e.Message; ;
        }
        try
        {
            if (ds.Tables.Contains("detBonif") == true)
            {
                xml = PasaDtAxml(ds.Tables["DetBonif"]);
                actualizaTabla(xml, distribuidora, "PA_NewDetBonif");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Detalle de Bonificaciones:" + e.Message; ;
        }
        try
        {
            if (ds.Tables.Contains("alcance") == true)
            {
                xml = PasaDtAxml(ds.Tables["alcance"]);
                actualizaTabla(xml, distribuidora, "PA_NewAlcance");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla alcance de Bonificaciones:" + e.Message; ;
        }
        return "";
    }


    [WebMethod]
    public string subirNewPrecios(string usuario, string clave, string imei, string xmlDatos)
    {
        string respuesta;
        DataSet ds;
        string xml;

        try
        {
            respuesta = validarConexion(usuario, clave, imei);
            if (respuesta.Length != 0)
            {
                return respuesta;
            }
        }
        catch (Exception ex)
        {
            return "validarConeccion en subirNewPrecios - " + ex.Message;
        }
        try
        {
            ds = CargaXmlaDataset(xmlDatos);
        }
        catch (Exception ex)
        {
            return "CargaXmlaDataset - Formato de xml no valido - " + ex.Message;
        }
        try
        {
            if (ds.Tables.Contains("cabListas") == true)
            {
                xml = PasaDtAxml(ds.Tables["CabListas"]);
                actualizaTabla(xml, distribuidora, "PA_NewCabListas");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Cabecera de Precios:" + e.Message; ;
        }
        try
        {
            if (ds.Tables.Contains("detListas") == true)
            {
                xml = PasaDtAxml(ds.Tables["DetListas"]);
                actualizaTabla(xml, distribuidora, "PA_NewDetListas");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Detalle de Precios:" + e.Message; ;
        }
        return "";
    }


    //[WebMethod]
    //public string bajarDatosMobile(string usuario, string clave, string imei)
    //{
    //    string respuesta = validarConexion(usuario, clave, imei);
    //    string xmlSalida = "";
    //    if (respuesta.Length != 0)
    //    {
    //        return respuesta;
    //    }
    //    try
    //    {
    //        ArrayList aTablas = new ArrayList();
    //        aTablas.Add("Articulos");
    //        aTablas.Add("cabListas");
    //        aTablas.Add("clientes");
    //        aTablas.Add("condvtas");
    //        aTablas.Add("configRuta");
    //        aTablas.Add("detListas");
    //        aTablas.Add("lineas");
    //        //aTablas.Add("localidades");
    //        aTablas.Add("ramos");
    //        aTablas.Add("rubros");
    //        aTablas.Add("rutaVentas");
    //        aTablas.Add("vendedores");
    //        aTablas.Add("Zonas");
    //        xmlSalida = pidedatos2(distribuidora, "Pa_BajadaMovil", imei, aTablas);

    //        string cDirectory = HttpContext.Current.Server.MapPath(@"xml\");
    //        string cFilenameDestino = "T" + imei + ".xml";
    //        cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, "_" + DateTime.Now.ToString("ddmmyyyy"));
    //        cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, "_" + DateTime.Now.Hour.ToString());
    //        cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, DateTime.Now.Minute.ToString());
    //        cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, DateTime.Now.Second.ToString());
    //        StreamWriter writer = File.AppendText(cDirectory + cFilenameDestino);
    //        writer.WriteLine(xmlSalida);
    //        writer.Close();

    //        if (xmlSalida.Length < 30)
    //            return xmlSalida;
    //        else
    //            //                return @"http://www.inteldevmobile.com.ar/preventa/xml/" + cFilenameDestino;
    //            //                return @"http://192.168.1.162:8888/preventa/xml/" + cFilenameDestino;
    //            //			return @"http://hergo2.no-ip.org:8888/preventa/xml/" + cFilenameDestino;
    //            return @"preventa/xml/" + cFilenameDestino;

    //    }
    //    catch
    //    {
    //        return "Error al bajar los datos";
    //    }


    //}

    [WebMethod]
    public string subirDatosMobile(string usuario, string clave, string imei, string xmlDatos)
    {
        string respuesta = validarConexion(usuario, clave, imei);

        if (respuesta.Length != 0)
        {
            return respuesta;
        }

        DataSet ds;
        try
        {
            ds = CargaXmlaDataset(xmlDatos);
        }
        catch
        {
            return "Formato de xml no valido";
        }





        string xml = "";
        SqlCommand CMD = new SqlCommand();
        CMD.CommandType = CommandType.StoredProcedure;
        CMD.Connection = _dao.getConnection();
        _dao.Open(CMD.Connection);

        SqlTransaction trans = CMD.Connection.BeginTransaction();
        try
        {
            CMD.Transaction = trans;
            SqlParameter param = new SqlParameter();
            param.ParameterName = "@distri";
            param.Value = distribuidora;
            param.Direction = ParameterDirection.Input;

            bool entro = true;
            try
            {
                if (ds.Tables["caboper"].Rows.Count == 0)
                    entro = false;
                else
                    xml = PasaDtAxml(ds.Tables["caboper"]);

            }
            catch
            {
                entro = false;
            }
            if (entro)
            {
                CMD.Parameters.Add(param);
                SqlParameter param1 = new SqlParameter();
                param1.ParameterName = "@xmldoc";
                param1.Value = xml;
                param1.Direction = ParameterDirection.Input;
                CMD.CommandTimeout = tiempotimeout;
                CMD.Parameters.Add(param1);
                CMD.CommandText = "PA_subeCaboper";
                CMD.ExecuteNonQuery();
                CMD.Parameters.Clear();
            }

            entro = true;
            try
            {
                if (ds.Tables["detoper"].Rows.Count == 0)
                    entro = false;
                else
                    xml = PasaDtAxml(ds.Tables["detoper"]);

            }
            catch
            {
                entro = false;
            }
            if (entro)
            {
                //SqlParameter param = new SqlParameter();
                //param.ParameterName = "@distri";
                //param.Value = distribuidora;
                //param.Direction = ParameterDirection.Input;
                CMD.Parameters.Add(param);
                SqlParameter param2 = new SqlParameter();
                param2.ParameterName = "@xmldoc";
                param2.Value = xml;
                param2.Direction = ParameterDirection.Input;
                CMD.CommandTimeout = tiempotimeout;
                CMD.Parameters.Add(param2);
                CMD.CommandText = "PA_SubeDetoper";

                CMD.ExecuteNonQuery();
            }

            if (!entro)
            {
                trans.Rollback();
                CMD.Dispose();
                _dao.Close(CMD.Connection);
                return "Error grabando los pedidos en el servidor";
            }

            trans.Commit();
        }
        catch (SqlException e)
        {
            trans.Rollback();
            CMD.Dispose();
            _dao.Close(CMD.Connection);
            return "Error al grabar datos: " + e.Message;
        }

        CMD.Dispose();
        _dao.Close(CMD.Connection);
        return "";

    }


    [WebMethod]
    public string bajarDatosCliente2(string usuario, string clave, string imei)
    {
        string respuesta;
        try
        {
            respuesta = validarConexion(usuario, clave, imei);
        }
        catch
        {
            return "Verificando servidor";
        }

        string xmlSalida = "";
        if (respuesta.Length != 0)
        {
            return respuesta;
        }

        try
        {
            ArrayList aTablas = new ArrayList();
            aTablas.Add("caboper");
            aTablas.Add("detoper");

            xmlSalida = pidedatos2(distribuidora, "Pa_BajadaCliente2", imei, aTablas);
        }
        catch
        {
            return "Error al bajar los datos";
        }
        return xmlSalida;
    }

    [WebMethod]
    public string bajarDatosCliente3(string usuario, string clave, string imei)
    {
        string respuesta;
        try
        {
            respuesta = validarConexion(usuario, clave, imei);
        }
        catch
        {
            return "Verificando servidor";
        }

        string xmlSalida = "";
        if (respuesta.Length != 0)
        {
            return respuesta;
        }

        try
        {
            ArrayList aTablas = new ArrayList();
            aTablas.Add("caboper");
            aTablas.Add("detoper");
            aTablas.Add("oferoper");

            xmlSalida = pidedatos2(distribuidora, "Pa_BajadaCliente3", imei, aTablas);
        }
        catch
        {
            return "Error al bajar los datos";
        }
        return xmlSalida;
    }


    


    [WebMethod]
    public string subirDatosCliente2(string usuario, string clave, string imei, string xmlDatos)
    {

        string respuesta;
        try
        {
            respuesta = validarConexion(usuario, clave, imei);
        }
        catch
        {
            return "Verificando servidor";
        }

        if (respuesta.Length != 0)
        {
            return respuesta;
        }
        DataSet ds;
        try
        {
            ds = CargaXmlaDataset(xmlDatos);
        }
        catch
        {
            return "Formato de xml no valido";
        }


        string xml = PasaDtAxml(ds.Tables["Rubros"]);
        try
        {
            actualizaTabla(xml, distribuidora, "PA_ActRubros");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Rubros: " + e.Message;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["Lineas"]);
            actualizaTabla(xml, distribuidora, "PA_ActLineas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Lineas:" + e.Message; ;
        }



        try
        {
            xml = PasaDtAxml(ds.Tables["Condvtas"]);
            actualizaTabla(xml, distribuidora, "PA_ActCondVtas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Condevtas:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["Zonas"]);
            actualizaTabla(xml, 1, "PA_ActZonas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Zonas:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["Vendedores"]);
            actualizaTabla(xml, distribuidora, "PA_ActVendedores");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Vendedores:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["Articulos"]);
            actualizaTabla(xml, distribuidora, "PA_ActArticulos");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Articulos:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["CabListas"]);
            actualizaTabla(xml, distribuidora, "PA_ActCabListas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla CabListas:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["DetListas"]);
            actualizaTabla(xml, distribuidora, "PA_ActDetListas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla DetListas:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["RutaVentas"]);
            actualizaTabla(xml, distribuidora, "PA_ActRutaVtas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla RutaVtas:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["ConfigRutas"]);
            actualizaTabla(xml, distribuidora, "PA_ActConfigRutas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla ConfigRuta:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["Ramos"]);
            actualizaTabla(xml, distribuidora, "PA_ActRamos");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Ramos:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["Clientes"]);
            actualizaTabla(xml, distribuidora, "PA_ActClientes");

        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Clientes:" + e.Message; ;
        }


        try
        {
            if (ds.Tables.Contains("cabBonif") == true)
            {
                xml = PasaDtAxml(ds.Tables["CabBonif"]);
                actualizaTabla(xml, distribuidora, "PA_ActCabBonif");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Cabecera de Bonificaciones:" + e.Message; ;
        }


        try
        {
            if (ds.Tables.Contains("detBonif") == true)
            {
                xml = PasaDtAxml(ds.Tables["DetBonif"]);
                actualizaTabla(xml, distribuidora, "PA_ActDetBonif");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Detalle de Bonificaciones:" + e.Message; ;
        }


        try
        {
            if (ds.Tables.Contains("tipoBonif") == true)
            {
                xml = PasaDtAxml(ds.Tables["tipoBonif"]);
                actualizaTabla(xml, distribuidora, "PA_ActTipoBonif");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla tipo de Bonificaciones:" + e.Message; ;
        }


        try
        {
            if (ds.Tables.Contains("alcance") == true)
            {
                xml = PasaDtAxml(ds.Tables["alcance"]);
                actualizaTabla(xml, distribuidora, "PA_ActAlcance");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla alcance de Bonificaciones:" + e.Message; ;
        }


        try
        {
            if (ds.Tables.Contains("stock") == true)
            {
                xml = PasaDtAxml(ds.Tables["stock"]);
                actualizaTabla(xml, distribuidora, "PA_ActStock");
            }
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Stock:" + e.Message; ;
        }


        return "";

    }

    //[WebMethod]
    //public string bajarDatosMobile2(string usuario, string clave, string imei)
    //{
    //    string respuesta = validarConexion(usuario, clave, imei);
    //    string xmlSalida = "";
    //    if (respuesta.Length != 0)
    //    {
    //        return respuesta;
    //    }

    //    try
    //    {
    //        ArrayList aTablas = new ArrayList();
    //        aTablas.Add("Articulos");
    //        aTablas.Add("cabListas");
    //        aTablas.Add("clientes");
    //        aTablas.Add("condvtas");
    //        aTablas.Add("configRuta");
    //        aTablas.Add("detListas");
    //        aTablas.Add("lineas");
    //        //aTablas.Add("localidades");
    //        aTablas.Add("ramos");
    //        aTablas.Add("rubros");
    //        aTablas.Add("rutaVentas");
    //        aTablas.Add("vendedores");
    //        aTablas.Add("Zonas");
    //        aTablas.Add("cabBonif");
    //        aTablas.Add("detbonif");
    //        aTablas.Add("tipoBonif");
    //        aTablas.Add("alcance");
    //        aTablas.Add("numeraciones");
    //        aTablas.Add("syscondivas");
    //        aTablas.Add("systasasiva");
    //        aTablas.Add("stock");

    //        xmlSalida = pidedatos2(distribuidora, "Pa_BajadaMovil2", imei, aTablas);

    //        string cDirectory = HttpContext.Current.Server.MapPath(@"xml\");
    //        string cFilenameDestino = "T" + imei + ".xml";
    //        cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, "_" + DateTime.Now.ToString("ddmmyyyy"));
    //        cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, "_" + DateTime.Now.Hour.ToString());
    //        cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, DateTime.Now.Minute.ToString());
    //        cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, DateTime.Now.Second.ToString());
    //        StreamWriter writer = File.AppendText(cDirectory + cFilenameDestino);
    //        writer.WriteLine(xmlSalida);
    //        writer.Close();

    //        if (xmlSalida.Length < 30)
    //            return xmlSalida;
    //        else
    //            //return @"http://www.inteldevmobile.com.ar/preventa/xml/" + cFilenameDestino;
    //            //return @"http://192.168.1.162:8888/preventa/xml/" + cFilenameDestino;
    //            //return @"http://hergo2.no-ip.org:8888/preventa/xml/" + cFilenameDestino;
    //            return @"preventa/xml/" + cFilenameDestino;

    //    }
    //    catch
    //    {
    //        return "Error al bajar los datos";
    //    }


    //}


    [WebMethod]
    public string subirDatosCliente3(string usuario, string clave, string imei, string xmlDatos)
    {

        string respuesta;
        try
        {
            respuesta = validarConexion(usuario, clave, imei);
        }
        catch
        {
            return "Verificando servidor";
        }

        if (respuesta.Length != 0)
        {
            return respuesta;
        }

        DataSet ds;
        try
        {
            ds = CargaXmlaDataset(xmlDatos);
        }
        catch
        {
            return "Formato de xml no valido";
        }

        string xml = PasaDtAxml(ds.Tables["Rubros"]);
        try
        {
            xml = PasaDtAxml(ds.Tables["Rubros"]);
            actualizaTabla(xml, distribuidora, "PA_ActRubros");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Rubros: " + e.Message;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["Lineas"]);
            actualizaTabla(xml, distribuidora, "PA_ActLineas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Lineas:" + e.Message; ;
        }

        try
        {
            xml = PasaDtAxml(ds.Tables["Canales"]);
            actualizaTabla(xml, distribuidora, "PA_ActCanales");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Canales:" + e.Message; ;
        }

        //try
        //{
        //    xml = PasaDtAxml(ds.Tables["Param"]);
        //    actualizaTabla(xml, distribuidora, "PA_ActParam");
        //}
        //catch (SqlException e)
        //{
        //    return "Error al cargar tabla Param:" + e.Message; ;
        //}

        try
        {
            xml = PasaDtAxml(ds.Tables["Condvtas"]);
            actualizaTabla(xml, distribuidora, "PA_ActCondVtas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla CondVtas:" + e.Message; ;
        }


        //try
        //{
        //    xml = PasaDtAxml(ds.Tables["Zonas"]);
        //    actualizaTabla(xml, 1, "PA_ActZonas");
        //}
        //catch (SqlException e)
        //{
        //    return "Error al cargar tabla Zonas:" + e.Message; ;
        //}


        //try
        //{
        //    xml = PasaDtAxml(ds.Tables["Vendedores"]);
        //    actualizaTabla(xml, distribuidora, "PA_ActVendedores");
        //}
        //catch (SqlException e)
        //{
        //    return "Error al cargar tabla Vendedores:" + e.Message; ;
        //}


        try
        {
            xml = PasaDtAxml(ds.Tables["Articulos"]);
            actualizaTabla(xml, distribuidora, "PA_ActArticulos2");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Articulos:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["CabListas"]);
            actualizaTabla(xml, distribuidora, "PA_ActCabListas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla CabListas:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["DetListas"]);
            actualizaTabla(xml, distribuidora, "PA_ActDetListas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla DetListas:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["RutaVentas"]);
            actualizaTabla(xml, distribuidora, "PA_ActRutaVtas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla RutaVtas:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["ConfigRutas"]);
            actualizaTabla(xml, distribuidora, "PA_ActConfigRutas");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla ConfigRuta:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["Ramos"]);
            actualizaTabla(xml, distribuidora, "PA_ActRamos");
        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Ramos:" + e.Message; ;
        }


        try
        {
            xml = PasaDtAxml(ds.Tables["Clientes"]);
            actualizaTabla(xml, distribuidora, "PA_ActClientes2");

        }
        catch (SqlException e)
        {
            return "Error al cargar tabla Clientes:" + e.Message; ;
        }




        return "";

    }

    
    [WebMethod]
    public string bajarDatosMobileZip(string usuario, string clave, string imei)
    {
        string respuesta = validarConexion(usuario, clave, imei);
        string xmlSalida = "";
        if (respuesta.Length != 0)
        {
            return respuesta;
        }

        try
        {
            ArrayList aTablas = new ArrayList();
            aTablas.Add("Articulos");
            aTablas.Add("cabListas");
            aTablas.Add("clientes");
            aTablas.Add("condvtas");
            aTablas.Add("configRuta");
            aTablas.Add("detListas");
            aTablas.Add("lineas");
            //aTablas.Add("localidades");
            aTablas.Add("ramos");
            aTablas.Add("rubros");
            aTablas.Add("rutaVentas");
            aTablas.Add("vendedores");
            aTablas.Add("Zonas");
            aTablas.Add("param");
            aTablas.Add("canales");
            aTablas.Add("oferesp");
            aTablas.Add("ofer_det");
            aTablas.Add("cabBonif");
            aTablas.Add("detBonif");
            aTablas.Add("alcance");
            aTablas.Add("stock");

            xmlSalida = pidedatosZip(distribuidora, "Pa_BajadaMovil3parte1", imei, aTablas);

            return xmlSalida;

        }
        catch
        {
            return "Error al bajar los datos";
        }

    }

    //[WebMethod]
    //public byte[] bajarDatosMobile3parte1ZIP(string usuario, string clave, string imei)
    //{
    //    string fileXml = bajarDatosMobile3parte1(usuario, clave, imei);
    //    return ComprimirArchivo(@"c:\" + fileXml.Replace("/", @"\"));
    //}
    //[WebMethod]
    //public byte[] bajarDatosMobile3parte2ZIP(string usuario, string clave, string imei)
    //{
    //    string fileXml = bajarDatosMobile3parte2(usuario, clave, imei);
    //    return ComprimirArchivo(@"c:\" + fileXml.Replace("/", @"\"));
    //}
    public byte[] ComprimirArchivo(string file)
    {
        byte[] outzip = null;
        FileStream sourceFile = File.OpenRead(file);
        DeflateStream defs = new DeflateStream(sourceFile, CompressionMode.Compress);
        defs.Read(outzip, 0, (int)sourceFile.Length);
        defs.Close();
        sourceFile.Close();

        return outzip;
    }

    [WebMethod]
    public string subirDatosMobile2(string usuario, string clave, string imei, string xmlDatos)
    {
        string respuesta = validarConexion(usuario, clave, imei);

        if (respuesta.Length != 0)
        {
            return respuesta;
        }

        DataSet ds;
        try
        {
            ds = CargaXmlaDataset(xmlDatos);
        }
        catch
        {
            return "Formato de xml no valido";
        }

        string xml = "";
        SqlCommand CMD = new SqlCommand();
        CMD.CommandType = CommandType.StoredProcedure;
        CMD.Connection = _dao.getConnection();
        _dao.Open(CMD.Connection);

        SqlTransaction trans = CMD.Connection.BeginTransaction();
        try
        {
            CMD.Transaction = trans;
            SqlParameter param = new SqlParameter();
            param.ParameterName = "@distri";
            param.Value = distribuidora;
            param.Direction = ParameterDirection.Input;


            bool entro = true;
            try
            {
                if (ds.Tables["caboper"].Rows.Count == 0)
                    entro = false;
                else
                    xml = PasaDtAxml(ds.Tables["caboper"]);

            }
            catch
            {
                entro = false;
            }
            if (entro)
            {


                CMD.Parameters.Add(param);
                SqlParameter param1 = new SqlParameter();
                param1.ParameterName = "@xmldoc";
                param1.Value = xml;
                param1.Direction = ParameterDirection.Input;
                CMD.CommandTimeout = tiempotimeout;
                CMD.Parameters.Add(param1);
                CMD.CommandText = "PA_subeCaboper2";
                CMD.ExecuteNonQuery();
                CMD.Parameters.Clear();
            }



            entro = true;
            try
            {
                if (ds.Tables["detoper"].Rows.Count == 0)
                    entro = false;
                else
                    xml = PasaDtAxml(ds.Tables["detoper"]);

            }
            catch
            {
                entro = false;
            }
            if (entro)
            {

                CMD.Parameters.Add(param);
                SqlParameter param2 = new SqlParameter();
                param2.ParameterName = "@xmldoc";
                param2.Value = xml;
                param2.Direction = ParameterDirection.Input;
                CMD.CommandTimeout = tiempotimeout;
                CMD.Parameters.Add(param2);
                CMD.CommandText = "PA_SubeDetoper2";

                CMD.ExecuteNonQuery();
                CMD.Parameters.Clear();
            }

            if (!entro)
            {
                trans.Rollback();
                CMD.Dispose();
                _dao.Close(CMD.Connection);
                return "Error grabando los pedidos en el servidor";
            }



            entro = true;
            try
            {
                if (ds.Tables["oferOper"].Rows.Count == 0)
                    entro = false;
                else
                    xml = PasaDtAxml(ds.Tables["oferOper"]);

            }
            catch
            {
                entro = false;
            }



            if (entro)
            {
                try
                {

                    CMD.Parameters.Add(param);
                    SqlParameter param3 = new SqlParameter();
                    param3.ParameterName = "@xmldoc";
                    param3.Value = xml;
                    param3.Direction = ParameterDirection.Input;
                    CMD.CommandTimeout = tiempotimeout;
                    CMD.Parameters.Add(param3);
                    CMD.CommandText = "PA_SubeOferOper";

                    CMD.ExecuteNonQuery();
                    CMD.Parameters.Clear();
                }
                catch (SqlException e)
                {
                    trans.Rollback();
                    CMD.Dispose();
                    _dao.Close(CMD.Connection);
                    return e.Message;
                }
            }

            trans.Commit();

            CMD.Dispose();


            entro = true;
            try
            {
                if (ds.Tables["numeraciones"].Rows.Count == 0)
                    entro = false;
                else
                    xml = PasaDtAxml(ds.Tables["numeraciones"]);

            }
            catch
            {
                entro = false;
            }
            if (entro)
            {
                SqlCommand CMD1 = new SqlCommand();
                try
                {

                    CMD1.Connection = _dao.getConnection();
                    CMD1.CommandType = CommandType.StoredProcedure;
                    SqlParameter param3 = new SqlParameter();
                    param3.ParameterName = "@xmldoc";
                    param3.Value = xml;
                    param3.Direction = ParameterDirection.Input;
                    CMD1.CommandTimeout = tiempotimeout;
                    CMD1.Parameters.Add(param3);
                    CMD1.CommandText = "PA_SubeNumeraciones";
                    SqlParameter param4 = new SqlParameter();
                    param4.ParameterName = "@distri";
                    param4.Value = distribuidora;
                    param4.Direction = ParameterDirection.Input;
                    CMD1.Parameters.Add(param4);
                    _dao.Open(CMD1.Connection);
                    CMD1.ExecuteNonQuery();
                    CMD1.Dispose();
                }
                catch
                {
                    CMD1.Dispose();
                }
                finally
                {
                    _dao.Close(CMD1.Connection);//nuevo.. no estaba tampoco
                }
            }




        }
        catch (SqlException e)
        {
            trans.Rollback();
            CMD.Dispose();
            _dao.Close(CMD.Connection);
            return "Error al grabar datos: " + e.Message;
        }
        //conexion.desconectar();
        return "";

    }

    


    [WebMethod]
    public string execQueryReturn(string miQuery)
    {
        SqlCommand cmdSql = new SqlCommand();
        cmdSql.Connection = _dao.getConnection();
        cmdSql.CommandText = miQuery;
        try
        {
            _dao.Open(cmdSql.Connection);
        }
        catch (Exception e)
        {
            return e.Message;
        }

        SqlDataReader dr = cmdSql.ExecuteReader();
        DataTable dt = new DataTable();
        dt.Load(dr);

        DataSet ds = new DataSet();
        ds.Tables.Add(dt);
        StringWriter sw = new StringWriter();
        ds.WriteXml(sw, XmlWriteMode.WriteSchema);


        dr.Close();
        dr.Dispose();
        try
        {
            _dao.Close(cmdSql.Connection);
        }
        catch (Exception e)
        {
            //return e.Message;

        }
        return sw.ToString();
        //return ds.Tables[0];

    }



    [WebMethod]
    public string borraXml(string cFile)
    {
        //    try
        //    {
        string cFileName = Path.GetFileName(cFile);
        string cDirectoryOrigen = HttpContext.Current.Server.MapPath(@"xml\");
        string cDirectoryDestino = HttpContext.Current.Server.MapPath(@"xml\enviados\");
        File.Move(cDirectoryOrigen + cFileName, cDirectoryDestino + cFileName);
        return "";
        //    }
        //    catch
        //    {
        //        return "Error al eliminiar archivo";
        //    }
    }

    

    [WebMethod]
    public string CunsultarPorCodigoDeBarra2(string codigobarra)
    {
        //codigobarra = "350";
        //var artprec = CunsultarPorCodigoDeBarra(codigoBarra);

        //return artprec.Descripcion;

        return "Parametro: " + codigobarra;
    }

    [WebMethod]
    public string devuelvefecha()
    {
        int difhora = 0;
        DateTime aux;
        aux = DateTime.Now;
        aux.AddHours(difhora);
        return aux.ToString();
    }

    [WebMethod]
    public string ActualizarPosicion(string usuario, string fecha, string lat, string lng, int estado, string cliente, int motivonocompra)
    {
        string resultado = "ok";
        //Comentar
        try
        {
            string cmd = "insert into posiciongps (usuario,fecha,latitud,longitud,estado,cliente,motivonocompra)values(@pusuario,@pfecha,@plat,@plng,@pestado,@pCliente,@pMotivonocompra)";
            SqlCommand insertar = new SqlCommand(cmd, _dao.getConnection());
            insertar.CommandType = CommandType.Text;
            insertar.Parameters.AddWithValue("@pusuario", usuario);
            insertar.Parameters.AddWithValue("@pfecha", fecha);
            insertar.Parameters.AddWithValue("@plat", double.Parse(lat));
            insertar.Parameters.AddWithValue("@plng", double.Parse(lng));
            insertar.Parameters.AddWithValue("@pestado", estado);
            insertar.Parameters.AddWithValue("@pCliente", cliente);
            insertar.Parameters.AddWithValue("@pMotivonocompra", motivonocompra);
            _dao.Open(insertar.Connection);
            insertar.ExecuteNonQuery();
            _dao.Close(insertar.Connection);
        }
        catch (Exception exc)
        {
            resultado = exc.Message;
        }
        //fin comentar

        if (resultado == null)
            resultado = "error";
        return resultado;
    }

    [WebMethod]
    public string ActualizarPosicionTest(string usuario, string fecha, string lat, string lng, int estado, string cliente, int motivonocompra, string pesos, int bultos)
    {
        string resultado = "ok";
        //Comentar
        try
        {
            string cmd = "insert into posiciongps (usuario,fecha,latitud,longitud,estado,cliente,motivonocompra,pesos,bultos)values(@pusuario,@pfecha,@plat,@plng,@pestado,@pCliente,@pMotivonocompra,@pPesos,@pBultos)";
            SqlCommand insertar = new SqlCommand(cmd, _dao.getConnection());
            insertar.CommandType = CommandType.Text;
            insertar.Parameters.AddWithValue("@pusuario", usuario);
            insertar.Parameters.AddWithValue("@pfecha", fecha);
            insertar.Parameters.AddWithValue("@plat", double.Parse(lat));
            insertar.Parameters.AddWithValue("@plng", double.Parse(lng));
            insertar.Parameters.AddWithValue("@pestado", estado);
            insertar.Parameters.AddWithValue("@pCliente", cliente);
            insertar.Parameters.AddWithValue("@pMotivonocompra", motivonocompra);
            insertar.Parameters.AddWithValue("@pBultos", bultos);
            insertar.Parameters.AddWithValue("@pPesos", float.Parse(pesos));
            _dao.Open(insertar.Connection);
            insertar.ExecuteNonQuery();
            _dao.Close(insertar.Connection);
        }
        catch (Exception exc)
        {
            resultado = exc.Message;
        }
        //fin comentar

        if (resultado == null)
            resultado = "error";
        return resultado;
    }

    

    

    [WebMethod]
    public DataTable ObtenerPosicionActualPreventistas()
    {

        DateTime hoy = DateTime.Today;

        //        string cmd = @"select * from posiciongps as a
        //                       where fecha>=@pFecha and
        //                       fecha in (
        //                       select max(fecha) as fecha
        //                       from posiciongps as b
        //                       where a.usuario=b.usuario
        //                       group by usuario)";

        //        string cmd = @"select a.* from posiciongps as a
        //                        inner join (
        //                        select usuario,max(fecha) as fecha
        //                                       from posiciongps
        //                                       where fecha>=@pFecha
        //                                       group by usuario) as b
        //                                       on a.usuario=b.usuario and a.fecha=b.fecha";
        string cmd = @"select a.*, v.empresa
                       from posiciongps a
                            inner join (   select usuario,max(fecha) as fecha
                                           from posiciongps
                                           where fecha>=@pFecha
                                           group by usuario) as b
                            on a.usuario=b.usuario and a.fecha=b.fecha
                            inner join vendedores v on a.usuario=v.usuario
                            where v.borrado=0";

        SqlCommand cmdSelect = new SqlCommand(cmd, _dao.getConnection());
        cmdSelect.CommandType = CommandType.Text;
        cmdSelect.Parameters.AddWithValue("@pfecha", hoy);
        _dao.Open(cmdSelect.Connection);
        //SqlDataReader dr = cmdSelect.ExecuteReader();
        DataTable dt = new DataTable("PosicionesGPS");
        dt.Load(cmdSelect.ExecuteReader());
        _dao.Close(cmdSelect.Connection);
        return dt;
    }

    [WebMethod]
    public DataTable ObtenerPosicionesDelPreventista(string usuario, DateTime fechaDesde, DateTime fechaHasta)
    {
        string cmd = @"select * from posiciongps where usuario = @pUsuario and fecha >= @pFechaDesde and fecha < @pFechaHasta";

        //        string cmd = @"select * from posiciongps as a
        //                       where fecha>=@pFecha and
        //                       fecha in (
        //                       select max(fecha) as fecha
        //                       from posiciongps as b
        //                       where a.usuario=b.usuario
        //                       group by usuario)";


        SqlCommand cmdSelect = new SqlCommand(cmd, conexion.conection);
        cmdSelect.CommandType = CommandType.Text;
        cmdSelect.Parameters.AddWithValue("@pFechaDesde", fechaDesde);
        cmdSelect.Parameters.AddWithValue("@pFechaHasta", fechaHasta);
        cmdSelect.Parameters.AddWithValue("@pUsuario", usuario);
        conexion.conectar();
        SqlDataReader dr = cmdSelect.ExecuteReader();

        DataTable dt = new DataTable("PosicionesGPS");
        dt.Load(dr);

        conexion.desconectar();
        return dt;
    }

    [WebMethod]
    public DataTable ObtenerVendedoresPorDia(DateTime fechaDesde, DateTime fechaHasta)
    {
        //        string cmd = @"select p.usuario, COUNT(p.visitados) as visitados, count(p.compradores) as compradores, COUNT(p.bultos) as bultos, COUNT(p.pesos) as pesos, v.empresa as empresa
        //                       from posiciongps p
        //                       inner join vendedores v on p.usuario=v.usuario
        //                       where fecha >= @pFechaDesde and fecha < @pFechaHasta
        //                       group by usuario";

        string cmd = @"select p.usuario, COUNT(p.visitados) as visitados, count(p.compradores) as compradores, COUNT(p.bultos) as bultos, COUNT(p.pesos) as pesos, v.empresa as empresa
                       from posiciongps p
                       inner join vendedores v on p.usuario=v.usuario
                       where fecha >= @pFechaDesde and fecha < @pFechaHasta and v.borrado=0
                       group by p.usuario,v.empresa";

        SqlCommand cmdSelect = new SqlCommand(cmd, conexion.conection);
        cmdSelect.CommandType = CommandType.Text;
        cmdSelect.Parameters.AddWithValue("@pFechaDesde", fechaDesde);
        cmdSelect.Parameters.AddWithValue("@pFechaHasta", fechaHasta);
        conexion.conectar();
        SqlDataReader dr = cmdSelect.ExecuteReader();

        DataTable dt = new DataTable("PosicionesGPS");
        dt.Load(dr);

        conexion.desconectar();
        return dt;
    }

    [WebMethod]
    public DataTable ObtenerPrimerClienteVisitado(string usuario, DateTime fechaDesde, DateTime fechaHasta)
    {
        string cmd = @"select top(1) cliente, fecha
                       from posiciongps
                       where usuario = @pUsuario and
	                   (fecha>=@pFechaDesde and fecha<@pFechaHasta)
	                   and cliente<>''";

        //        string cmd = @"select * from posiciongps as a
        //                       where fecha>=@pFecha and
        //                       fecha in (
        //                       select max(fecha) as fecha
        //                       from posiciongps as b
        //                       where a.usuario=b.usuario
        //                       group by usuario)";


        SqlCommand cmdSelect = new SqlCommand(cmd, conexion.conection);
        cmdSelect.CommandType = CommandType.Text;
        cmdSelect.Parameters.AddWithValue("@pUsuario", usuario);
        cmdSelect.Parameters.AddWithValue("@pFechaDesde", fechaDesde);
        cmdSelect.Parameters.AddWithValue("@pFechaHasta", fechaHasta);
        conexion.conectar();
        SqlDataReader dr = cmdSelect.ExecuteReader();

        DataTable dt = new DataTable("PosicionesGPS");
        dt.Load(dr);

        conexion.desconectar();
        return dt;
    }

    [WebMethod]
    public DataTable ObtenerPreventistasPorEmpresa(string empresa)
    {
        string cmd = @"select usuario from vendedores where borrado=0 and empresa=@pEmpresa";

        //        string cmd = @"select * from posiciongps as a
        //                       where fecha>=@pFecha and
        //                       fecha in (
        //                       select max(fecha) as fecha
        //                       from posiciongps as b
        //                       where a.usuario=b.usuario
        //                       group by usuario)";


        SqlCommand cmdSelect = new SqlCommand(cmd, conexion.conection);
        cmdSelect.CommandType = CommandType.Text;
        cmdSelect.Parameters.AddWithValue("@pEmpresa", empresa);
        conexion.conectar();
        SqlDataReader dr = cmdSelect.ExecuteReader();

        DataTable dt = new DataTable("Vendedores");
        dt.Load(dr);

        conexion.desconectar();
        return dt;
    }

    

    

    



    public void actualizaTabla(string xml, int distri, string PA)
    {

        SqlCommand CMD = new SqlCommand(PA, _dao.getConnection());
        CMD.CommandType = CommandType.StoredProcedure;
        SqlParameter param = new SqlParameter();
        param.ParameterName = "@distri";
        param.Value = distri;
        param.Direction = ParameterDirection.Input;
        CMD.Parameters.Add(param);
        SqlParameter param1 = new SqlParameter();
        param1.ParameterName = "@xmldoc";
        param1.Value = xml;
        param1.Direction = ParameterDirection.Input;
        CMD.CommandTimeout = tiempotimeout;
        CMD.Parameters.Add(param1);
        _dao.Open(CMD.Connection);
        CMD.ExecuteNonQuery();
        _dao.Close(CMD.Connection);
        //Desconectar();
    }

    public void actualizaTablaTransaction(string xml, int distri, SqlCommand CMD)
    {

        SqlParameter param = new SqlParameter();
        param.ParameterName = "@distri";
        param.Value = distri;
        param.Direction = ParameterDirection.Input;
        CMD.Parameters.Add(param);
        SqlParameter param1 = new SqlParameter();
        param1.ParameterName = "@xmldoc";
        param1.Value = xml;
        param1.Direction = ParameterDirection.Input;
        CMD.CommandTimeout = tiempotimeout;
        CMD.Parameters.Add(param1);
        //conexion.conectar();
        CMD.ExecuteNonQuery();
        //conexion.desconectar();
        //Desconectar();
    }

    public string pidedatos(int distri, string PA, string imei)
    {

        SqlCommand CMD = new SqlCommand(PA, _dao.getConnection());
        CMD.CommandType = CommandType.StoredProcedure;
        SqlParameter param = new SqlParameter();
        param.ParameterName = "@distri";
        param.Value = distri;
        param.Direction = ParameterDirection.Input;
        CMD.Parameters.Add(param);
        CMD.CommandTimeout = tiempotimeout;

        SqlParameter param1 = new SqlParameter();
        param1.ParameterName = "@imei";
        param1.Value = imei;
        param1.Direction = ParameterDirection.Input;
        CMD.Parameters.Add(param1);
        CMD.CommandTimeout = tiempotimeout;

        _dao.Open(CMD.Connection);
        SqlDataReader dr = CMD.ExecuteReader();
        DataSet ds = new DataSet();
        DataTable dtCabOper = new DataTable();
        DataTable dtDetOper = new DataTable();
        dtCabOper.TableName = "caboper";
        dtDetOper.TableName = "detoper";
        ds.Tables.Add(dtCabOper);
        ds.Tables.Add(dtDetOper);

        ds.Load(dr, LoadOption.OverwriteChanges, dtCabOper, dtDetOper);

        StringWriter sw = new StringWriter();
        ds.WriteXml(sw, XmlWriteMode.WriteSchema);
        dr.Close();
        dr.Dispose();
        _dao.Close(CMD.Connection);
        //Desconectar();
        if (dtCabOper.Rows.Count + dtDetOper.Rows.Count == 0)
        {
            return "Nada para descargar";
        }
        else
        {
            return sw.ToString();
        }
    }
    public string pidedatos3(int distri, string PA, string imei, ArrayList aTablas)
    {
        SqlCommand CMD = new SqlCommand(PA, _dao.getConnection());
        CMD.CommandType = CommandType.StoredProcedure;
        SqlParameter param = new SqlParameter();
        param.ParameterName = "@distri";
        param.Value = distri;
        param.Direction = ParameterDirection.Input;
        CMD.Parameters.Add(param);
        CMD.CommandTimeout = tiempotimeout;

        SqlParameter param1 = new SqlParameter();
        param1.ParameterName = "@imei";
        param1.Value = imei;
        param1.Direction = ParameterDirection.Input;
        CMD.Parameters.Add(param1);
        CMD.CommandTimeout = tiempotimeout;

        DataSet ds = new DataSet();
        DataTable[] adt = new DataTable[aTablas.Count];
        string[] asTablas = (string[])aTablas.ToArray(typeof(string));

        foreach (string cTabla in aTablas)
        {
            ds.Tables.Add(new DataTable(cTabla));
        }

        _dao.Open(CMD.Connection);
        SqlDataReader dr = CMD.ExecuteReader();
        ds.Load(dr, LoadOption.Upsert, asTablas);
        dr.Close();
        dr.Dispose();

        string jsonSalida = DataSetToJsonObj(ds);
        //leer todo y pasarlo a json
        _dao.Close(CMD.Connection);
        return jsonSalida;
    }

    public string DataSetToJsonObj(DataSet ds)
    {
        StringBuilder JsonString = new StringBuilder();
        if (ds != null && ds.Tables.Count > 0)
        {
            JsonString.Append("{");
            foreach (DataTable tabla in ds.Tables)
            {
                JsonString.Append("\"" + tabla.TableName + "\":[");
                for (int i = 0; i < tabla.Rows.Count; i++)
                {
                    JsonString.Append("{");
                    for (int j = 0; j < tabla.Columns.Count; j++)
                    {
                        if (j < tabla.Columns.Count - 1)
                        {
                            JsonString.Append("\"" + tabla.Columns[j].ColumnName.ToString() + "\":" + "\"" + tabla.Rows[i][j].ToString() + "\",");
                        }
                        else if (j == tabla.Columns.Count - 1)
                        {
                            JsonString.Append("\"" + tabla.Columns[j].ColumnName.ToString() + "\":" + "\"" + tabla.Rows[i][j].ToString() + "\"");
                        }
                    }
                    if (i == tabla.Rows.Count - 1)
                    {
                        JsonString.Append("}");
                    }
                    else
                    {
                        JsonString.Append("},");
                    }
                }
                JsonString.Append("]");
            }
            JsonString.Append("}");
            return JsonString.ToString();
        }
        else
        {
            return null;
        }
    }
    


    public string pidedatosZip(int distri, string PA, string imei, ArrayList aTablas)
    {
        //Application.Lock();
        SqlCommand CMD = new SqlCommand(PA, _dao.getConnection());
        CMD.CommandType = CommandType.StoredProcedure;
        SqlParameter param = new SqlParameter();
        param.ParameterName = "@distri";
        param.Value = distri;
        param.Direction = ParameterDirection.Input;
        CMD.Parameters.Add(param);
        CMD.CommandTimeout = tiempotimeout;

        SqlParameter param1 = new SqlParameter();
        param1.ParameterName = "@imei";
        param1.Value = imei;
        param1.Direction = ParameterDirection.Input;
        CMD.Parameters.Add(param1);
        CMD.CommandTimeout = tiempotimeout;



        DataSet ds = new DataSet();
        int registros = 0;
        DataTable[] adt = new DataTable[aTablas.Count];
        string[] asTablas = (string[])aTablas.ToArray(typeof(string));

        foreach (string cTabla in aTablas)
        {
            ds.Tables.Add(new DataTable(cTabla));

            //adt[ds.Tables.Count-1].TableName = cTabla;

        }

        _dao.Open(CMD.Connection);
        SqlDataReader dr = CMD.ExecuteReader();
        ds.Load(dr, LoadOption.Upsert, asTablas);
        dr.Close();
        dr.Dispose();
        //StringWriter sw = new StringWriter();

        //ds.WriteXml(sw, XmlWriteMode.WriteSchema);

        string zip = ZipDataset(ds, false).ToString();

        _dao.Close(CMD.Connection);
        //Desconectar();

        foreach (DataTable dt in ds.Tables)
        {
            registros += dt.Rows.Count;
        }

        if (registros == 0)
        {
            //Application.UnLock();
            return "Nada para descargar";
        }
        else
        {
            //Application.UnLock();
            return zip;
        }

    }

    public byte[] ZipDataset(DataSet ds, bool bIgnoreSchema)
    {
        byte[] bb = null;
        using (MemoryStream ms = new MemoryStream())
        {
            using (GZipStream zip = new GZipStream(ms, CompressionMode.Compress))
            {
                ds.WriteXml(zip, bIgnoreSchema ? System.Data.XmlWriteMode.IgnoreSchema : XmlWriteMode.WriteSchema);
                zip.Close();
            }
            bb = ms.GetBuffer();
            ms.Close();
        }

        return bb;
    }
    private string[,] tabletoarray(DataTable dt)
    {
        string[,] stringArray = new string[dt.Rows.Count, dt.Columns.Count];

        for (int row = 0; row < dt.Rows.Count; ++row)
        {
            for (int col = 0; col < dt.Columns.Count; col++)
            {
                stringArray[row, col] = dt.Rows[row][col].ToString();
            }
        }
        return stringArray;
    }
}

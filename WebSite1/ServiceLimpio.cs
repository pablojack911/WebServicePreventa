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
                string query = "select iddistribuidora from sysimeis where imei=@imei and borrado<>1 and suspendido<>1";
                SqlCommand cmdSql = CrearSqlCommand(con, query, CommandType.Text, new SqlParameter("@imei", imei));
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
    public string AltaPda(string imei, int iddistribuidora, string comentario)
    {
        string res = string.Empty;
        try
        {
            using (SqlConnection connection = new SqlConnection(stringConnection))
            {
                string query = "insert into sysImeis(imei,iddistribuidora,comentario) values(@imei, @iddistribuidora, @comentario)";
                SqlParameter[] parameters = { new SqlParameter("@imei", imei), new SqlParameter("@iddistribuidora", iddistribuidora), new SqlParameter("@comentario", comentario) };
                SqlCommand command = CrearSqlCommand(connection, query, CommandType.Text, parameters);
                connection.Open();
                res = Convert.ToString(command.ExecuteNonQuery());
            }
        }
        catch (Exception ex)
        {
            return "AltaPda: " + ex.Message;
        }
        return "";
    }

    [WebMethod]
    public string IniciarSesion(string usuario, string clave, string imei)
    {
        string respuesta = validarConexion(usuario, clave, imei);
        if (respuesta.Length != 0)
        {
            return respuesta;
        }
        try
        {
            using (SqlConnection connection = new SqlConnection(stringConnection))
            {
                string query = "select * from vendedores where usuario=@usuario and pass=@pass";
                SqlParameter[] parameters = { new SqlParameter("@usuario", usuario), new SqlParameter("@pass", clave) };
                SqlCommand command = CrearSqlCommand(connection, query, CommandType.Text, parameters);
                connection.Open();
                using (SqlDataReader dr = command.ExecuteReader())
                {
                    if (dr.Read())
                        return "";
                }
            }
        }
        catch (Exception ex)
        {
            return "IniciarSesion - " + ex.Message;
        }
        return "User o password incorrecto";
    }

    [WebMethod]
    public int ActualizarLastDownload(string imei, string fecha)
    {
        int res = 0;
        //Comentar
        try
        {
            using (SqlConnection connection = new SqlConnection(stringConnection))
            {
                string query = "update sysImeis set lastDownload = @pFecha where imei = @pImei";
                SqlParameter[] parameters = { new SqlParameter("@pImei", imei), new SqlParameter("@pFecha", fecha) };
                SqlCommand command = CrearSqlCommand(connection, query, CommandType.Text, parameters);
                connection.Open();
                res = command.ExecuteNonQuery();
            }
        }
        catch
        {
            res = -1;
        }
        //fin comentar
        return res;
    }

    [WebMethod]
    public int RevivirPedidosVendedor(string idVendedor)
    {
        int res = 0;
        //Comentar
        try
        {
            using (SqlConnection connection = new SqlConnection(stringConnection))
            {
                string query = "revivePedidos";
                SqlParameter[] parameters = { new SqlParameter("@pventista", idVendedor) };
                SqlCommand command = CrearSqlCommand(connection, query, CommandType.StoredProcedure, parameters);
                connection.Open();
                res = command.ExecuteNonQuery();
            }
        }
        catch
        {
            res = -1;
        }
        //fin comentar
        return res;
    }

    [WebMethod]
    public int ActualizarLastDownloadATodas(string fecha)
    {
        int res = 0;
        try
        {
            using (SqlConnection connection = new SqlConnection(stringConnection))
            {
                string query = "update sysImeis set lastDownload = @pFecha where comentario not like 'PC%'";
                SqlParameter[] parameters = { new SqlParameter("@pFecha", fecha) };
                SqlCommand command = CrearSqlCommand(connection, query, CommandType.Text, parameters);
                connection.Open();
                res = command.ExecuteNonQuery();
            }
        }
        catch
        {
            res = -1;
        }
        //fin comentar
        return res;
    }

    [WebMethod]
    public string ActualizarPosicionTestImei(string usuario, string fecha, string lat, string lng, int estado, string cliente, int motivonocompra, string pesos, int bultos, string imei)
    {
        string resultado = "ok";
        //Comentar
        try
        {
            using (SqlConnection connection = new SqlConnection(stringConnection))
            {
                string query = "insert into posiciongps (usuario,fecha,latitud,longitud,estado,cliente,motivonocompra,pesos,bultos,imei)values(@pusuario,@pfecha,@plat,@plng,@pestado,@pCliente,@pMotivonocompra,@pPesos,@pBultos,@pImei)";
                SqlCommand command = CrearSqlCommand(connection, query, CommandType.Text);
                command.Parameters.AddWithValue("@pusuario", usuario);
                command.Parameters.AddWithValue("@pfecha", fecha);
                command.Parameters.AddWithValue("@plat", double.Parse(lat));
                command.Parameters.AddWithValue("@plng", double.Parse(lng));
                command.Parameters.AddWithValue("@pestado", estado);
                command.Parameters.AddWithValue("@pCliente", cliente);
                command.Parameters.AddWithValue("@pMotivonocompra", motivonocompra);
                command.Parameters.AddWithValue("@pBultos", bultos);
                command.Parameters.AddWithValue("@pPesos", float.Parse(pesos));
                command.Parameters.AddWithValue("@pImei", imei);
                connection.Open();
                command.ExecuteNonQuery();
            }
        }
        catch (Exception exc)
        {
            resultado = exc.Message;
        }
        if (resultado == null)
            resultado = "error";
        return resultado;
    }

    [WebMethod]
    public string actualizaFechaDownLoad(string usuario, string clave, string imei)
    {
        string respuesta = validarConexion(usuario, clave, imei);
        if (respuesta.Length != 0)
        {
            return respuesta;
        }
        try
        {
            using (SqlConnection con = new SqlConnection(stringConnection))
            {
                SqlCommand cmdSql = CrearSqlCommand(con, "PA_ActImeis", CommandType.StoredProcedure, new SqlParameter("@imei", imei));
                con.Open();
                cmdSql.ExecuteNonQuery();
            }
        }
        catch
        {
            return "Error al actualizar Fecha de Bajada";
        }
        return "";
    }

    [WebMethod]
    public string bajarDatosCliente4(string usuario, string clave, string imei)
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
            aTablas.Add("caboper");
            aTablas.Add("detoper");
            aTablas.Add("oferoper");
            aTablas.Add("adt_pedidos");
            xmlSalida = pidedatos2(distribuidora, "Pa_BajadaCliente4", imei, aTablas);
        }
        catch (Exception ex)
        {
            return "bajarDatosCliente4 - Error al bajar los datos - " + ex.Message;
        }
        return xmlSalida;
    }

    [WebMethod]
    public string bajarDatosMobile4(string usuario, string clave, string imei)
    {
        string respuesta = validarConexion(usuario, clave, imei);
        string xmlSalida = "";
        if (respuesta.Length != 0)
        {
            return respuesta;
        }

        try
        {
            ArrayList aTablas = new ArrayList
            {
                "Articulos",
                "cabListas",
                "clientes",
                "condvtas",
                "configRuta",
                "detListas",
                "lineas",
                "ramos",
                "rubros",
                "rutaVentas",
                "vendedores",
                "Zonas",
                "param",
                "canales",
                "barras",
                "oferesp",
                "ofer_det",
                "cabBonif",
                "detBonif",
                "alcance",
                "stock"
            };

            /*
            TablaXmlArticulo tablaXmlArticulo = new TablaXmlArticulo();
            TablaXmlCabLista tablaXmlCabListas = new TablaXmlCabLista();
            TablaXmlCliente tablaXmlCliente = new TablaXmlCliente();
            TablaXmlCondVta tablaXmlCondVta = new TablaXmlCondVta();
            TablaXmlConfigRuta tablaXmlConfigRuta = new TablaXmlConfigRuta();
            TablaXmlDetLista tablaXmlDetLista = new TablaXmlDetLista();
            TablaXmlLinea tablaXmlLinea = new TablaXmlLinea();
            TablaXmlRamo tablaXmlRamo = new TablaXmlRamo();
            TablaXmlRubro tablaXmlRubro = new TablaXmlRubro();
            TablaXmlRutaVenta tablaXmlRutaVenta = new TablaXmlRutaVenta();
            TablaXmlVendedor tablaXmlVendedor = new TablaXmlVendedor();
            TablaXmlOferEsp tablaXmlOferEsp = new TablaXmlOferEsp();
            TablaXmlOfer_Det tablaXmlOfer_det = new TablaXmlOfer_Det();
            TablaXmlCabBonif tablaXmlCabBonif = new TablaXmlCabBonif();
            TablaXmlDetBonif tablaXmlDetBonif = new TablaXmlDetBonif();
            TablaXmlAlcance tablaXmlAlcance = new TablaXmlAlcance();
            TablaXmlStock tablaXmlStock = new TablaXmlStock();
            TablaXmlBarras tablaXmlBarras = new TablaXmlBarras();*/

            xmlSalida = pidedatos2(distribuidora, "Pa_BajadaMovil4", imei, aTablas);

            string cDirectory = HttpContext.Current.Server.MapPath(@"xml\");
            string cFilenameDestino = "T" + imei + ".xml";
            cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, "_" + DateTime.Now.ToString("ddmmyyyy"));
            cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, "_" + DateTime.Now.Hour.ToString());
            cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, DateTime.Now.Minute.ToString());
            cFilenameDestino = cFilenameDestino.Insert(cFilenameDestino.Length - 4, DateTime.Now.Second.ToString());
            StreamWriter writer = File.AppendText(cDirectory + cFilenameDestino);
            writer.WriteLine(xmlSalida);
            writer.Close();

            if (xmlSalida.Length < 30)
                return xmlSalida;
            else
                //return @"http://www.inteldevmobile.com.ar/preventa/xml/" + cFilenameDestino;
                //return @"http://192.168.1.162:8888/preventa/xml/" + cFilenameDestino;
                //return @"http://hergo2.no-ip.org:8888/preventa/xml/" + cFilenameDestino;
                return @"preventa/xml/" + cFilenameDestino;
        }
        catch
        {
            return "Error al bajar los datos";
        }
    }

    [WebMethod]
    public string dameDistri(string imei)
    {
        int distri = 0;
        using (SqlConnection con = new SqlConnection(stringConnection))
        {
            string query = "select iddistribuidora from sysimeis where imei =@imei";
            SqlParameter[] parameters = { new SqlParameter("@imei", imei.Trim()) };
            SqlCommand cmdSql = CrearSqlCommand(con, query, CommandType.Text, parameters);
            con.Open();
            using (SqlDataReader drUsuario = cmdSql.ExecuteReader())
            {
                if (drUsuario.Read())
                {
                    distri = Convert.ToInt32(drUsuario["iddistribuidora"]);
                }
                else
                {
                    distribuidora = 0;
                }
            }
        }
        return distri.ToString();
    }

    [WebMethod]
    public string subirDatosMobile3(string usuario, string clave, string imei, string xmlDatos)
    {
        string xml = "";
        DataSet ds;
        string respuesta = validarConexion(usuario, clave, imei);
        if (respuesta.Length != 0)
        {
            return respuesta;
        }
        SqlParameter paramDistri = new SqlParameter("@distri", dameDistri(imei));
        try
        {
            ds = CargaXmlaDataset(xmlDatos);
        }
        catch (Exception ex)
        {
            return "subirDatosMobile3 - Formato de xml no valido" + ex.Message;
        }

        try
        {
            using (SqlConnection connection = new SqlConnection(stringConnection))
            {
                connection.Open();
                SqlTransaction sqlTran = connection.BeginTransaction();

                SqlCommand command = CrearSqlCommand(connection, "", CommandType.StoredProcedure);
                command.Transaction = sqlTran;

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
                    command.CommandText = "PA_subeCaboper2";
                    command.Parameters.Add(paramDistri);
                    command.Parameters.Add(new SqlParameter("@xmldoc", xml));
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
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
                    command.CommandText = "PA_SubeDetoper3";
                    command.Parameters.Add(paramDistri);
                    command.Parameters.Add(new SqlParameter("@xmldoc", xml));
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }

                if (!entro)
                {
                    sqlTran.Rollback();
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
                        command.CommandText = "PA_SubeOferOper";
                        command.Parameters.Add(paramDistri);
                        command.Parameters.Add(new SqlParameter("@xmldoc", xml));
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                    }
                    catch (SqlException e)
                    {
                        sqlTran.Rollback();
                        return e.Message;
                    }
                }
                sqlTran.Commit();
            }
            //using (SqlConnection connection = new SqlConnection(stringConnection))
            //{
            //    connection.Open();
            //    SqlTransaction sqlTran = connection.BeginTransaction();

            //    SqlCommand command = CrearSqlCommand(connection, "", CommandType.StoredProcedure);
            //    command.Transaction = sqlTran;
            //    bool entro = true;
            //    try
            //    {
            //        if (ds.Tables["numeraciones"].Rows.Count == 0)
            //            entro = false;
            //        else
            //            xml = PasaDtAxml(ds.Tables["numeraciones"]);
            //    }
            //    catch
            //    {
            //        entro = false;
            //    }
            //    if (entro)
            //    {
            //        try
            //        {
            //            command.CommandText = "PA_SubeNumeraciones";
            //            command.Parameters.Add(new SqlParameter("@xmldoc", xml));
            //            command.Parameters.Add(paramDistri);
            //            command.ExecuteNonQuery();
            //            command.Parameters.Clear();
            //        }
            //        catch
            //        {
            //        }
            //    }

            //    entro = true;
            //    try
            //    {
            //        if (ds.Tables["adt_pedidos"].Rows.Count == 0)
            //            entro = false;
            //        else
            //            xml = PasaDtAxml(ds.Tables["adt_pedidos"]);
            //    }
            //    catch
            //    {
            //        entro = false;
            //    }
            //    if (entro)
            //    {
            //        try
            //        {
            //            command.CommandText = "PA_SubeAdt_pedidos";
            //            command.Parameters.Add(new SqlParameter("@xmldoc", xml));
            //            command.Parameters.Add(paramDistri);
            //            command.ExecuteNonQuery();
            //        }
            //        catch
            //        {
            //        }
            //    }
            //}
        }
        catch (SqlException e)
        {
            return "Error al grabar datos: " + e.Message;
        }
        return "";
    }

    [WebMethod]
    public string SubirFotoCliente(string usuario, string cliente, string foto)
    {
        string result = "ok";
        try
        {
            byte[] baFoto = Convert.FromBase64String(foto);
            MemoryStream ms = new MemoryStream(baFoto);
            Image returnImage = Image.FromStream(ms);
            string cDirectoryDestino = HttpContext.Current.Server.MapPath(@"Fotos\");
            string path = cDirectoryDestino + cliente + ".jpeg";
            returnImage.Save(path, System.Drawing.Imaging.ImageFormat.Jpeg);
            using (SqlConnection connection = new SqlConnection(stringConnection))
            {
                string query = "insert into fotosclientes (usuario,idcliente,pathfoto)values(@pusuario,@pCliente,@pFoto)";
                SqlCommand command = CrearSqlCommand(connection, query, CommandType.Text);
                command.Parameters.AddWithValue("@pusuario", usuario);
                command.Parameters.AddWithValue("@pCliente", cliente);
                command.Parameters.AddWithValue("@pFoto", path);

                connection.Open();
                command.ExecuteNonQuery();
            }
        }
        catch (Exception exc)
        {
            result = exc.Message;
        }
        return result;
    }

    [WebMethod]
    public string hola(string mensaje)
    {
        return "Hola " + mensaje;
    }

    public string pidedatos2(int distri, string PA, string imei, ArrayList aTablas)
    {
        using (SqlConnection con = new SqlConnection(stringConnection))
        {
            SqlParameter[] parameters = { new SqlParameter("@distri", distri), new SqlParameter("@imei", imei) };
            SqlCommand command = CrearSqlCommand(con, PA, CommandType.StoredProcedure, parameters);
            DataSet ds = new DataSet();
            int registros = 0;
            DataTable[] adt = new DataTable[aTablas.Count];
            string[] asTablas = (string[])aTablas.ToArray(typeof(string));

            foreach (string cTabla in aTablas)
            {
                ds.Tables.Add(new DataTable(cTabla));

                //adt[ds.Tables.Count-1].TableName = cTabla;

            }
            con.Open();
            using (SqlDataReader dr = command.ExecuteReader())
            {
                ds.Load(dr, LoadOption.Upsert, asTablas);
            }
            StringWriter sw = new StringWriter();
            ds.WriteXml(sw, XmlWriteMode.WriteSchema);

            foreach (DataTable dt in ds.Tables)
            {
                registros += dt.Rows.Count;
            }

            if (registros == 0)
            {
                return "Nada para descargar";
            }
            else
            {
                return sw.ToString();
            }
        }
    }

    private DataSet CargaXmlaDataset(string xmlString)
    {
        DataSet ds = new DataSet();

        ds.ReadXml(new System.IO.StringReader(xmlString), XmlReadMode.InferSchema);
        ds.AcceptChanges();
        return ds;
    }

    private string PasaDtAxml(DataTable dt)
    {
        StringWriter sWriter = new StringWriter();
        dt.WriteXml(sWriter, XmlWriteMode.WriteSchema);
        return sWriter.ToString();
    }

    private void pendientes(string imei, string usuario)
    {
        using (SqlConnection con = new SqlConnection(stringConnection))
        {
            SqlParameter[] parameters = { new SqlParameter("@imei", imei), new SqlParameter("@usuario", usuario) };
            SqlCommand cmdSql = CrearSqlCommand(con, "PA_metependiente", CommandType.StoredProcedure, parameters);
            con.Open();
            cmdSql.ExecuteNonQuery();
        }
    }

    private SqlCommand CrearSqlCommand(SqlConnection connection, string sp, CommandType tipo, params SqlParameter[] parameters)
    {
        SqlCommand command = connection.CreateCommand();
        command.CommandText = sp;
        command.CommandType = tipo;
        command.CommandTimeout = 3600;
        if (parameters.Length > 0)
            command.Parameters.AddRange(parameters);
        return command;
    }
}
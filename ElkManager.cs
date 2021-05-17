using Elasticsearch.Net;
using Nest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Web;

namespace PROJECT_NAME.Models
{
    public enum DBType
    {
        PROD,
        TEST,
        LOCAL
    }

    public static class Extensions
    {
        public static string GetCert(this DBType dBType)
        {
            string certPath = null;
            switch (dBType)
            {
                case DBType.PROD:
                    certPath = "YOUR_CERT_FILE_PATH";
                    return File.Exists(certPath) ? certPath : null;
                case DBType.TEST:
                    certPath = "YOUR_CERT_FILE_PATH";
                    return File.Exists(certPath) ? certPath : null;
                case DBType.LOCAL:
                    certPath = "NOT_NEEDED";
                    return certPath;
                default:
                    return certPath;
            }
        }

        public static Uri[] GetUris(this DBType dBType)
        {
            switch (dBType)
            {
                case DBType.PROD:
                    return new[]
                    {
                        new Uri("https://YOUR_URL_01:PORT_NUMBER"),
                        new Uri("https://YOUR_URL_02:PORT_NUMBER"),
                        new Uri("https://YOUR_URL_02:PORT_NUMBER"),
                    };
                case DBType.TEST:
                    return new[]
                    {
                        new Uri("https://YOUR_URL_01:PORT_NUMBER"),
                        new Uri("https://YOUR_URL_02:PORT_NUMBER"),
                        new Uri("https://YOUR_URL_02:PORT_NUMBER"),
                    };
                case DBType.LOCAL:
                    return new[]
                    {
                        new Uri("http://localhost:PORT_NUMBER"),
                    };
                default:
                    return null;
            }
        }
    }

    public class ElkManager<T> where T : class
    {
        private DBType _dBType;
        private string _index;
        private string _user;
        private string _pwd;
        private ElasticClient _elkClient;

        public static bool _ConectionSucccess = false;

        public ElkManager(DBType dBType, string index, string user, string pwd)
        {
            _dBType = dBType;
            _index = index;
            _user = user;
            _pwd = pwd;

            try
            {
                _elkClient = GetConnection(_dBType, _index, _user, _pwd);

                if (_elkClient.Ping().IsValid)
                {
                    _ConectionSucccess = true;
                }
                else
                {
                    _ConectionSucccess = false;
                }
            }
            catch
            {
                _ConectionSucccess = false;
            }
            
        }

        private ElasticClient GetConnection(DBType dBType, string index, string user, string pwd)
        {
            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            
            var uris = dBType.GetUris();
            string certPath = dBType.GetCert();

            if (certPath != null)
            {
                var connectionPool = new SniffingConnectionPool(uris);

                var settings = new ConnectionSettings(connectionPool);
                settings.RequestTimeout(TimeSpan.FromMinutes(10));

                if (certPath != "NOT_NEEDED")
                {
                    X509Certificate cert = new X509Certificate(certPath);
                    settings.ServerCertificateValidationCallback(CertificateValidations.AuthorityIsRoot(cert));
                }
                else
                {
                    ServicePointManager.ServerCertificateValidationCallback = new RemoteCertificateValidationCallback(delegate { return true; });
                }
                
                if((uris != null) && uris.Count() == 1)
                {
                    var pool = new SingleNodeConnectionPool(uris.First());
                    var defaultIndex = index;
                    settings = new ConnectionSettings(pool);
                }
                
                settings.BasicAuthentication(user, pwd);
                settings.DefaultIndex(index);
                
                ElasticClient elkClient = new ElasticClient(settings);

                return elkClient;
            }
            else
            {
                return null;
            }
        }

        public bool ConnectionSuccess()
        {
            return _ConectionSucccess;
        }

        public List<T> FindAll()
        {

            if (_elkClient != null)
            {

                List<T> resultList = new List<T>();

                var searchResult = _elkClient.Search<T>(s => s
                    .From(0).Size(1000)
                    .Query(q => q.MatchAll())
                    .Scroll("5m"));

                if(searchResult.Total > 0)
                {
                    resultList.AddRange(searchResult.Documents);

                    var results = _elkClient.Scroll<T>("10m", searchResult.ScrollId);

                    while (results.Documents.Any())
                    {
                        resultList.AddRange(results.Documents);
                        results = _elkClient.Scroll<T>("10m", searchResult.ScrollId);
                    }
                }

                return resultList;

            }
            else
            {
                return null;
            }
        }

        public List<T> FindListById(string[] idList)
        {

            if (_elkClient != null)
            {

                if ((idList != null) && idList.Count() > 1)
                {
                    var findResponse = _elkClient.Search<T>(s => s
                    .From(0)
                    .Size(idList.Count())
                    .Index(_index)
                    .Query(q => q.Ids(i => i.Values(idList)))
                    //.Scroll("5m")
                    );

                    if (findResponse.Total > 0)
                    {
                        var result = findResponse.Documents.ToList();
                        return result;
                    }
                    else
                    {
                        return new List<T>();
                    }

                    //var results = elkClient.Scroll<T>("10m", findResponse.ScrollId);

                    //while (results.Documents.Any())
                    //{
                    //    var r = results.Fields;

                    //    results = elkClient.Scroll<T>("10m", results.ScrollId);
                    //}
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        public bool InsertMany(List<T> insertObjList, out string message)
        {

            if (_elkClient != null)
            {
                var bulkIndexResponse = _elkClient.Bulk(b => b
                    .Index(_index)
                    .IndexMany(insertObjList));

                if (bulkIndexResponse.IsValid && bulkIndexResponse.ApiCall.Success && bulkIndexResponse.DebugInformation.ToLower().Contains("valid nest response"))
                {
                    message = "Success";
                    return true;
                }
                else
                {
                    message = bulkIndexResponse.DebugInformation;
                    return false;
                }
            }
            else
            {
                message = "Cert File does not exists";
                return false;
            }
        }

        public bool UpdateMany(List<T> updateObjList, out string message)
        {

            string[] changedIds = updateObjList
                .Where(w => w.GetPropertyValue("ID") != null)
                .Select(s => (string)s.GetPropertyValue("ID"))
                .ToArray();

            if ((changedIds != null) && changedIds.Count() > 1)
            {
                var bulkResponse = _elkClient
                    .Bulk(b => b.UpdateMany<T>(
                        updateObjList,
                        (bulkDescriptor, doc) => bulkDescriptor.Doc(doc)));

                if (bulkResponse.IsValid && bulkResponse.ApiCall.Success && bulkResponse.DebugInformation.ToLower().Contains("valid nest response"))
                {
                    message = "Success";
                    return true;
                }
                else
                {
                    message = bulkResponse.DebugInformation;
                    return false;
                }
            }
            else
            {
                message = "Not Valid object list\nList must have ID as property";
                return false;
            }
        }

    }
}

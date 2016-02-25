using Dapper;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ENode
{
    static class DapperExtentions
    {
        private static readonly ConcurrentDictionary<Type, List<PropertyInfo>> _paramCache = new ConcurrentDictionary<Type, List<PropertyInfo>>();
        
        /// <summary>Insert data async into table.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="data"></param>
        /// <param name="table"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns>row(s) affected.</returns>
        public static Task<int> InsertToMySqlAsync(this IDbConnection connection, dynamic data, string table, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var obj = data as object;
            var properties = GetProperties(obj);
            var columns = string.Join(",", properties);
            var values = string.Join(",", properties.Select(p => "@" + p));
            var sql = string.Format("insert into {0} ({1}) values ({2})", table, columns, values);
            
            return connection.ExecuteAsync(sql, obj, transaction, commandTimeout);
        }

        private static List<string> GetProperties(object obj)
        {
            if (obj == null)
            {
                return new List<string>();
            }
            if (obj is DynamicParameters)
            {
                return (obj as DynamicParameters).ParameterNames.ToList();
            }
            return GetPropertyInfos(obj).Select(x => x.Name).ToList();
        }
        private static List<PropertyInfo> GetPropertyInfos(object obj)
        {
            if (obj == null)
            {
                return new List<PropertyInfo>();
            }

            List<PropertyInfo> properties;
            if (_paramCache.TryGetValue(obj.GetType(), out properties)) return properties.ToList();
            properties = obj.GetType().GetProperties(BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public).ToList();

            //Sequence is supposed to be auto increment.
            properties.RemoveAll(p => p.Name.Equals("Sequence"));

            _paramCache[obj.GetType()] = properties;
            

            return properties;
        }
    }
}

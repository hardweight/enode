using ECommon.Configurations;
using ENode.Commanding;
using ENode.Commanding.Impl;
using ENode.Eventing;
using ENode.Eventing.Impl;
using ENode.Infrastructure;
using ENode.Infrastructure.Impl.Mysql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ENode.Configurations
{
    public static class ENodeConfigurationExtensions
    {
        public static ENodeConfiguration UseMySqlCommandStore(this ENodeConfiguration configuration, OptionSetting optionSetting = null)
        {
            var _configuration = configuration.GetCommonConfiguration();
            _configuration.SetDefault<ICommandStore, MySqlCommandStore>(new MySqlCommandStore(optionSetting ?? new OptionSetting (
                new KeyValuePair<string,object>("ConnectionString",configuration.Setting.SqlDefaultConnectionString),
                new KeyValuePair<string, object>("TableName","Command"),
                new KeyValuePair<string, object>("PrimaryKeyName","PK_Command"))));

            return configuration;
        }

        public static ENodeConfiguration UseMySqlEventStore(this ENodeConfiguration configuration, OptionSetting optionSetting = null)
        {
            var _configuration = configuration.GetCommonConfiguration();
            _configuration.SetDefault<IEventStore, MySqlEventStore>(new MySqlEventStore(optionSetting ?? new OptionSetting(
                new KeyValuePair<string, object>("ConnectionString", configuration.Setting.SqlDefaultConnectionString),
                new KeyValuePair<string, object>("TableName", "EventStream"),
                new KeyValuePair<string, object>("PrimaryKeyName", "PK_EventStream"),
                new KeyValuePair<string, object>("CommandIndexName", "IX_EventStream_AggId_CommandId"),
                new KeyValuePair<string, object>("BulkCopyBatchSize", 1000),
                new KeyValuePair<string, object>("BulkCopyTimeout", 60))));

            return configuration;
        }

        /// <summary>Use the MySqlSequenceMessagePublishedVersionStore as the ISequenceMessagePublishedVersionStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMySqlSequenceMessagePublishedVersionStore(this ENodeConfiguration configuration, OptionSetting optionSetting = null)
        {
            var _configuration = configuration.GetCommonConfiguration();
            _configuration.SetDefault<ISequenceMessagePublishedVersionStore, MySqlSequenceMessagePublishedVersionStore>(new MySqlSequenceMessagePublishedVersionStore(optionSetting ?? new OptionSetting(
                new KeyValuePair<string, object>("ConnectionString", configuration.Setting.SqlDefaultConnectionString),
                new KeyValuePair<string, object>("TableName", "SequenceMessagePublishedVersion"),
                new KeyValuePair<string, object>("PrimaryKeyName", "PK_SequenceMessagePublishedVersion"))));
            return configuration;
        }
        /// <summary>Use the MySqlMessageHandleRecordStore as the IMessageHandleRecordStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMySqlMessageHandleRecordStore(this ENodeConfiguration configuration, OptionSetting optionSetting = null)
        {
            var _configuration = configuration.GetCommonConfiguration();
            _configuration.SetDefault<IMessageHandleRecordStore, MySqlMessageHandleRecordStore>(new MySqlMessageHandleRecordStore(optionSetting ?? new OptionSetting(
                new KeyValuePair<string, object>("ConnectionString", configuration.Setting.SqlDefaultConnectionString),
                new KeyValuePair<string, object>("OneMessageTableName", "MessageHandleRecord"),
                new KeyValuePair<string, object>("OneMessageTablePrimaryKeyName", "PK_MessageHandleRecord"),
                new KeyValuePair<string, object>("TwoMessageTableName", "TwoMessageHandleRecord"),
                new KeyValuePair<string, object>("TwoMessageTablePrimaryKeyName", "PK_TwoMessageHandleRecord"),
                new KeyValuePair<string, object>("ThreeMessageTableName", "ThreeMessageHandleRecord"),
                new KeyValuePair<string, object>("ThreeMessageTablePrimaryKeyName", "PK_ThreeMessageHandleRecord"))));
            return configuration;
        }
    }
}

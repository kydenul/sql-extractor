package sqlextractor

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kydenul/sql-extractor/internal/models"
)

func TestExtractor_RawSQL(t *testing.T) {
	t.Parallel()
	as := assert.New(t)
	extractor := NewExtractor("")

	sql := "SELECT * FROM users WHERE name = 'kyden'"
	extractor.SetRawSQL(sql)
	err := extractor.Extract()
	as.Nil(err)
	as.Equal(sql, extractor.RawSQL())
}

func TestExtractor_TemplatizeSQL(t *testing.T) {
	t.Parallel()
	as := assert.New(t)
	extractor := NewExtractor("")

	// single string param
	sql := "SELECT * FROM users WHERE name = 'kyden'"
	extractor.SetRawSQL(sql)
	err := extractor.Extract()

	as.Nil(err)
	as.Equal([]models.SQLOpType{models.SQLOperationSelect}, extractor.OpType())
	as.Equal([]string{"SELECT * FROM users WHERE name eq ?"}, extractor.TemplatizedSQL())
	as.Equal([][]any{{"kyden"}}, extractor.Params())
	as.Equal(
		[][]*models.TableInfo{{models.NewTableInfo("", "users", "", "users")}},
		extractor.TableInfos(),
	)
}

func TestExtractor_Params(t *testing.T) {
	t.Parallel()
	as := assert.New(t)

	// single string param
	sql := "SELECT * FROM users WHERE name = 'kyden'"
	extractor := NewExtractor(sql)
	err := extractor.Extract()
	as.Nil(err)
	as.Equal([]models.SQLOpType{models.SQLOperationSelect}, extractor.OpType())
	as.Equal([]string{"SELECT * FROM users WHERE name eq ?"}, extractor.TemplatizedSQL())
	as.Equal([][]any{{"kyden"}}, extractor.Params())
	as.Equal(
		[][]*models.TableInfo{{models.NewTableInfo("", "users", "", "users")}},
		extractor.TableInfos(),
	)

	// multiple params
	sql = "SELECT * FROM users WHERE name = 'kyden' AND age = 25 AND active = true"
	extractor.SetRawSQL(sql)
	err = extractor.Extract()
	as.Nil(err)
	as.Equal([]models.SQLOpType{models.SQLOperationSelect}, extractor.OpType())
	as.Equal(
		[]string{"SELECT * FROM users WHERE name eq ? and age eq ? and active eq ?"},
		extractor.TemplatizedSQL(),
	)
	as.Equal([][]any{{"kyden", int64(25), int64(1)}}, extractor.Params())
	as.Equal(
		[][]*models.TableInfo{{models.NewTableInfo("", "users", "", "users")}},
		extractor.TableInfos(),
	)

	// no params
	sql = "SELECT * FROM users"
	extractor.SetRawSQL(sql)
	err = extractor.Extract()
	as.Nil(err)
	as.Equal([]models.SQLOpType{models.SQLOperationSelect}, extractor.OpType())
	as.Equal([]string{"SELECT * FROM users"}, extractor.TemplatizedSQL())
	as.Equal(0, len(extractor.Params()[0]))
	as.Equal(
		[][]*models.TableInfo{{models.NewTableInfo("", "users", "", "users")}},
		extractor.TableInfos(),
	)
}

func TestExtractor_TableInfos(t *testing.T) {
	t.Parallel()
	as := assert.New(t)

	// single table
	sql := "SELECT * FROM users WHERE name = 'kyden'"
	extractor := NewExtractor(sql)
	err := extractor.Extract()
	as.Nil(err)
	as.Equal([]models.SQLOpType{models.SQLOperationSelect}, extractor.OpType())
	as.Equal([]string{"SELECT * FROM users WHERE name eq ?"}, extractor.TemplatizedSQL())
	as.Equal([][]any{{"kyden"}}, extractor.Params())
	as.Equal(
		[][]*models.TableInfo{{models.NewTableInfo("", "users", "", "users")}},
		extractor.TableInfos(),
	)

	// multiple tables
	sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.name = 'kyden'"
	extractor.SetRawSQL(sql)
	err = extractor.Extract()
	as.Nil(err)
	as.Equal([]models.SQLOpType{models.SQLOperationSelect}, extractor.OpType())
	as.Equal(
		[]string{
			"SELECT * FROM users AS u CROSS JOIN orders AS o ON u.id eq o.user_id WHERE u.name eq ?",
		},
		extractor.TemplatizedSQL(),
	)
	as.Equal([][]any{{"kyden"}}, extractor.Params())
	as.Equal([][]*models.TableInfo{
		{
			models.NewTableInfo("", "users", "", "users"),
			models.NewTableInfo("", "orders", "", "orders"),
		},
	}, extractor.TableInfos())
}

func TestExtractor_Extract_Error(t *testing.T) {
	t.Parallel()
	as := assert.New(t)
	extractor := NewExtractor("")

	// invalid SQL syntax
	sql := "SELECT * FROM WHERE name = 'kyden'"
	extractor.SetRawSQL(sql)
	err := extractor.Extract()
	as.Error(err)

	// empty SQL
	sql = ""
	extractor.SetRawSQL(sql)
	err = extractor.Extract()
	as.Error(err)
	as.Equal("empty SQL statement", err.Error())
}

func TestExtractor_TemplatizedSQLHash(t *testing.T) {
	t.Parallel()
	as := assert.New(t)

	// 测试默认哈希函数(sha256)
	sql := "SELECT * FROM users WHERE name = 'kyden'"
	extractor := NewExtractor(sql)
	err := extractor.Extract()
	as.Nil(err)

	// 获取默认哈希值
	hashes := extractor.TemplatizedSQLHash()
	as.Equal(1, len(hashes))

	// 手动计算预期的哈希值
	expectedHash := sha256.Sum256([]byte("SELECT * FROM users WHERE name eq ?"))
	expectedHashStr := hex.EncodeToString(expectedHash[:])
	as.Equal(expectedHashStr, hashes[0])

	// 测试自定义哈希函数(md5)
	customHashFn := func(data []byte) string {
		hash := md5.Sum(data)
		return hex.EncodeToString(hash[:])
	}

	customHashes := extractor.TemplatizedSQLHash(customHashFn)
	as.Equal(1, len(customHashes))

	// 手动计算预期的MD5哈希值
	expectedMD5Hash := md5.Sum([]byte("SELECT * FROM users WHERE name eq ?"))
	expectedMD5HashStr := hex.EncodeToString(expectedMD5Hash[:])
	as.Equal(expectedMD5HashStr, customHashes[0])

	// 测试多个SQL语句的情况
	multiSQL := "SELECT * FROM users; INSERT INTO logs (action) VALUES ('login')"
	extractor = NewExtractor(multiSQL)
	err = extractor.Extract()
	as.Nil(err)

	// 应该有两个模板化SQL和两个哈希值
	as.Equal(2, len(extractor.TemplatizedSQL()))
	multiHashes := extractor.TemplatizedSQLHash()
	as.Equal(2, len(multiHashes))

	// 验证每个哈希值
	for i, templatedSQL := range extractor.TemplatizedSQL() {
		hash := sha256.Sum256([]byte(templatedSQL))
		expectedHashStr = hex.EncodeToString(hash[:])
		as.Equal(expectedHashStr, multiHashes[i])
	}
}

func TestExtractor_ComplexQueries(t *testing.T) {
	t.Parallel()
	as := assert.New(t)
	extractor := NewExtractor("")

	// Join with conditions
	sql := "SELECT u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 18 AND o.amount > 100.50"
	extractor.SetRawSQL(sql)
	err := extractor.Extract()
	as.Nil(err)
	as.Equal([]models.SQLOpType{models.SQLOperationSelect}, extractor.OpType())
	as.Equal(
		[]string{
			"SELECT u.name, o.order_id FROM users AS u CROSS JOIN orders AS o ON u.id eq o.user_id WHERE u.age gt ? and o.amount gt ?",
		},
		extractor.TemplatizedSQL(),
	)
	as.Equal(2, len(extractor.Params()[0]))
	as.Equal([][]*models.TableInfo{
		{
			models.NewTableInfo("", "users", "", "users"),
			models.NewTableInfo("", "orders", "", "orders"),
		},
	}, extractor.TableInfos())

	t.Logf("raw SQL: %s\n Templatized SQL: %s \n TableInfos: %v \n Params: %v",
		extractor.RawSQL(), extractor.TemplatizedSQL(), extractor.TableInfos(), extractor.Params())

	// group by and having
	sql = "SELECT department, COUNT(*) as count FROM employees WHERE salary >= 50000 GROUP BY department HAVING count > 5"
	extractor.SetRawSQL(sql)
	err = extractor.Extract()
	as.Nil(err)
	as.Equal([]models.SQLOpType{models.SQLOperationSelect}, extractor.OpType())
	as.Equal(
		[]string{
			"SELECT department, COUNT(1) AS count FROM employees WHERE salary ge ? GROUP BY department HAVING count gt ?",
		},
		extractor.TemplatizedSQL(),
	)
	as.Equal([][]any{{int64(50000), int64(5)}}, extractor.Params())
	as.Equal(
		[][]*models.TableInfo{{models.NewTableInfo("", "employees", "", "employees")}},
		extractor.TableInfos(),
	)
}

func TestExtractor_0(t *testing.T) {
	as := assert.New(t)
	extractor := NewExtractor("")

	sql := `update tbGameCoinSerialV2 set iStatus =0,sRecvErrorNo='0',sRecvErrorNo2='',sRecvErrorMsg='',sDeductInfo ='curl \'http://12.34.56.78:80/sig=WuQqM7C9Q/ZGJIBdWzABlK4Z5Tk=\' -H\'Cookie: session_id=openid;session_type=kp_actoken; -H\'Content-Type: application/x-www-form-urlencoded\' -H\'From-Service: pay.midas.dq.deduct\' -H\'From-Span: 0-4-9-2\' -H\'X-Djc-Root-Us: 1749542023913528\'',sRecv='{\"ret\":0,\"billno\":\"ABCD-20250610155344095-060226402\",\"balance\":46280,\"gen_balance\":46280,\"used_gen_amt\":20}' where sSerialNum='ABCD-20250610155344095-060226402' and sPayType = 'dq_mxxxx'`
	extractor.SetRawSQL(sql)
	err := extractor.Extract()
	as.Nil(err)
	as.Equal(
		[]string{
			"UPDATE tbGameCoinSerialV2 SET iStatus eq ?, sRecvErrorNo eq ?, sRecvErrorNo2 eq ?, sRecvErrorMsg eq ?, sDeductInfo eq ?, sRecv eq ? WHERE sSerialNum eq ? and sPayType eq ?",
		},
		extractor.TemplatizedSQL(),
	)

	sql = "select lId,sBizCode,iGoodsId,iNum,sExt from dbUserCart_3.tbUserCart_67 where sUid='12020708613042267053' and sGoodsApp='9866' and sBizCode='ty' and sGroupKey ='areaid=170&platid=0&partition=70001&roleid=8070733106736623171'"
	extractor.SetRawSQL(sql)
	err = extractor.Extract()
	as.Nil(err)
	as.Equal(
		[]string{
			"SELECT lId, sBizCode, iGoodsId, iNum, sExt FROM dbUserCart_?.tbUserCart_? WHERE sUid eq ? and sGoodsApp eq ? and sBizCode eq ? and sGroupKey eq ?",
		},
		extractor.TemplatizedSQL(),
	)
	as.Equal([][]*models.TableInfo{
		{
			models.NewTableInfo("dbUserCart_3", "tbUserCart_67", "dbUserCart_?", "tbUserCart_?"),
		},
	}, extractor.TableInfos())
	as.Equal(4, len(extractor.Params()[0]))
	t.Log(extractor.templatedSQL, extractor.TemplatizedSQLHash())

	sql = "SELECT iMallId, iMallGroupId, iStatus, iPubStatus, dtCreateTime, dtLastUpdate, sBizCode, sActOwner, sLastOperator, dtSellBegin, dtSellEnd, dtShowBegin, dtShowEnd, iMallQty, sBuyLimit, iPromotePrice, iVipPromotePrice, dtPromoteStart, dtPromoteEnd, iVipPrice, iShipFree, iJDDeduct, sJDDeductInfo, sAwardInfo, iSupportCoupon, iTlpCheck, iGoodsId, iGoodsGroupLeader, iGoodsGroupId, iOriPrice, iPrice, sIPInfo, iSuppliersId, iActionId, iGoodsWeight, iStatus, sGoodsSN, sBarCode, iCatId, iTagId, iWaterMark, sWaterMarkInfo, iSortOrder, sMallName, sProfileImg, sDetailImg, sMallBrief, sKeywords, iSoldNum, sRelations, iSaleId, iSaleType, iCustomize, iWareId, iMallLeftId, sTuanInfo, iLv, sTLPSerial, iTlpCheck FROM dbX5Mall.tbMallInfo WHERE iMallId IN ('39', '40')"
	extractor.SetRawSQL(sql)
	extractor.Extract()
	t.Log(extractor.TemplatizedSQL())
}

func TestExtractor_1(t *testing.T) {
	t.Parallel()
	as := assert.New(t)

	sql := "SELECT sBizCode FROM dbUserRss.tbUserRss_1 WHERE sUid = ?"
	extractor := NewExtractor(sql)
	err := extractor.Extract()
	as.Nil(err)

	tsql := extractor.TemplatizedSQL()

	as.Equal([]string{"SELECT sBizCode FROM dbUserRss.tbUserRss_? WHERE sUid eq ?"}, tsql)
	as.Equal(
		[][]*models.TableInfo{
			{models.NewTableInfo("dbUserRss", "tbUserRss_1", "dbUserRss", "tbUserRss_?")},
		},
		extractor.TableInfos(),
	)
	as.Equal([]bool{true}, extractor.HasParamMarker())

	// Update
	sql = "update db_check_in.check_in_daily_record_20250630 set status=?,reward_rsp=?,reward_log=?,update_time=? where auto_id=? and user_id=? and record_key=?"
	extractor.SetRawSQL(sql)
	err = extractor.Extract()
	as.Nil(err)

	tsql = extractor.TemplatizedSQL()
	as.Equal(
		[]string{
			"UPDATE db_check_in.check_in_daily_record_? SET status eq ?, reward_rsp eq ?, reward_log eq ?, update_time eq ? WHERE auto_id eq ? and user_id eq ? and record_key eq ?",
		},
		tsql)
	as.Equal([][]*models.TableInfo{
		{
			models.NewTableInfo(
				"db_check_in",
				"check_in_daily_record_20250630",
				"db_check_in",
				"check_in_daily_record_?",
			),
		},
	}, extractor.TableInfos())
	as.Equal([]bool{true}, extractor.HasParamMarker())
}

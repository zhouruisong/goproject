// var url = "http://10.75.145.178:80";
var url = "";

var opdel = "<button type='button' class='button green' onclick='Del(this)'>删除</button>";
var opmod = "<button type='button' class='button green' onclick='Edit(this)'>修改</button>";
var opadd = "<button type='button' class='button green' onclick='Add(this)'>增加</button>";
// var op = opdel + opmod + opadd;
var totalop = opdel + opmod;

// 全局存储数据
var g_data = [];

// 每页显示多少行
var line_each_page = 20;

function Init() {
	var ip = window.location.host;
	var port = window.location.port;
	if (port.length == 0) {
		url = "http://" + ip + ":" + "80";
	} else {
		url = "http://" + ip + ":" + port;
	}
}

// 从服务器获取upserver数据刷新界面
function shownewtableup() {
	var reloadurl = url + "/" + "opUploadServer" + "?" + "op=" + "4" + "&ip=" + "0";
	$.ajax({
		url: reloadurl,
		type: 'get',
		success: function (data) {
		var JSON = $.parseJSON(data);
		updateList(JSON["result"], "showtableup");
		}
	});
}

// 从服务器获取node数据刷新界面
function shownewtablenode() {
	var reloadurl = url + "/" + "opUploadNode" + "?" + "op=" + "4" + "&nodenumber=" + "0";
	$.ajax({
		url: reloadurl,
		type: 'get',
		success: function (data) {
		var JSON = $.parseJSON(data);
		updateList(JSON["result"], "showtablenode");
		}
	});
}

// 判断是否有空值
function IsEmpty (mixed_var) {
    var key;
 
    if (mixed_var === "" || mixed_var === null 
    	|| mixed_var === false || typeof mixed_var === 'undefined') {
        return true;
    }
 
    if (typeof mixed_var == 'object') {
        for (key in mixed_var) {
            return false;
        }
        return true;
    }
 
    return false;
}

// 弹出确认按钮，确认添加和确认修改
function DoConfirm(obj) {
	var trlist = obj.parentNode.parentNode;
	var tdlist = trlist.getElementsByTagName("td");
	var len = tdlist.length - 1;

	var para = [];
	var i = 0;
	for (; i < len ; i++) {
		var input = tdlist[i].getElementsByTagName("input");
		para[i] = input[0].value;
		if (IsEmpty(para[i]))
		{
			window.alert("不能有空值！");
			return;
		}
	}

	// console.log(tdlist);
	var atitle = tdlist[len];
	var alist = atitle.childNodes;
	var theadname = obj.parentNode.parentNode.parentNode.parentNode;
	var tablenameid = theadname.getAttribute("id");

	var op = alist[0].getAttribute("op");
	// console.log(op);

	switch (op) {
		case '0':
			// console.log(para);
			var a=window.confirm("您确定要添加吗？");  
			if (a) {
				switch (tablenameid) {
					case "tableup":
						operateUploadServer(0, para);
						break;
					case "tablenode":
						operateUploadNode(0, para);
						break;
					default:
						break;
				}
			}else {
				trlist.parentNode.removeChild(trlist);
			}

			break;
		case '2':
			var a=window.confirm("您确定要修改吗？");  
			var oldvalue = alist[0].getAttribute("oldvalue").split(",");
			if (a) {
				para[i] = oldvalue[0];
				// console.log(para);
				switch (tablenameid) {
					case "tableup":
						operateUploadServer(2, para);
						break;
					case "tablenode":
						operateUploadNode(2, para);
						break;
					default:
						break;
				}
			} else {
				// window.alert("未修改！");
				var btn = tdlist[len];
				var alist = btn.childNodes;
				alist[0].setAttribute("onclick", "Del(this)");
				alist[0].innerText = "删除";

				// console.log(tdlist[0]);
				// console.log(oldvalue);
				// 不删除恢复数据
				for (var i = 0; i < len ; i++) {
					var input = tdlist[i].getElementsByTagName("input");
					// console.log(input);
					input[0].value = oldvalue[i];
					input[0].style.borderStyle = "";
					input[0].style.borderWidth = "";
					input[0].readOnly = true;
				}
			}
			break;
		default:
			// console.log(op);
			break;
	}
}

// 确认删除
function Del(obj) {
	var trlist = obj.parentNode.parentNode;
	var tdlist = trlist.getElementsByTagName("td");
	var nodenumber = tdlist[0].getElementsByTagName("input");

	var a=window.confirm("您确定要删除吗？");  
	if (a) {
		var tablename = obj.parentNode.parentNode.parentNode.parentNode;
		var tablenameid = tablename.getAttribute("id");
		switch (tablenameid) {
			case "tableup":
				operateUploadServer(1, nodenumber[0].value);
				break;
			case "tablenode":
				operateUploadNode(1, nodenumber[0].value);
				break;
			default:
				break;
		}
	}
} 

function Edit(obj) {
	var trlist = obj.parentNode.parentNode;
	// console.log(trlist);
	var tdlist = trlist.getElementsByTagName("td");

	var len = tdlist.length - 1;
	var i = 0;
	var oldvalue = [];
	for (; i < len ; i++) {
		var input = tdlist[i].getElementsByTagName("input");
		oldvalue[i] = input[0].value;
		// 表中值可以编辑
		input[0].readOnly = false;
		input[0].style.borderStyle = "solid";
		input[0].style.borderWidth = "thin";
	}

	var btn = tdlist[len];
	var alist = btn.childNodes;
	alist[0].setAttribute("onclick", "DoConfirm(this)");
	// console.log(nodenumber);
	alist[0].setAttribute("oldvalue", oldvalue);
	alist[0].setAttribute("op", 2);
	alist[0].innerText = "提交";
	// console.log(alist);
}

function Add() {
	var obj;
	var table;
	if (document.getElementById("showtablenode").style.display == "block") {
		obj = document.getElementById("showtablenode");
		table = document.getElementById("tablenode");
	} else if (document.getElementById("showtableup").style.display == "block") {
		obj = document.getElementById("showtableup");
		table = document.getElementById("tableup");
	}

	var tbody = table.getElementsByTagName("tbody");
	var trlist = tbody[0].childNodes;
	var len = trlist.length;
	var collen = trlist[0].childNodes.length - 1;
	// console.log(len);
	// console.log(collen);

	var newtr=document.createElement("tr");
	for (var  i = 0; i < collen; i++) {
		var input = trlist[len-1].childNodes[i].getElementsByTagName("input");
		if (input.length != 0) {
			var classstyle = input[0].getAttribute("class");
			var newtd=document.createElement("td");
			newtd.innerHTML = '<input class=' + '\"' + classstyle + '\"' + ' value=""/></td>';
			newtr.appendChild(newtd);
		}
	}

	// console.log(newtr);
	var newtd=document.createElement("td");
	newtd.innerHTML = totalop;
	newtr.appendChild(newtd);

	var alist = newtd.childNodes;
	alist[0].setAttribute("onclick", "DoConfirm(this)");
	alist[0].innerText = "提交";
	
	var td = newtr.getElementsByTagName("td");
	for (var  i = 0; i < td.length - 1; i++) {
		var input = td[i].getElementsByTagName("input");
		input[0].style.borderStyle = "solid";
		input[0].style.borderWidth = "thin";
	}
	
	tbody[0].appendChild(newtr);
	alist[0].setAttribute("op", 0);
}

function prepage(obj) {
	var lilist = obj.parentNode.parentNode.getElementsByTagName("li");
	// console.log(lilist[1]);

	var alist = lilist[1].childNodes;
	var value = alist[0].text;
	// console.log(value);
	var nextpage = parseInt(value) - 1;
	if (nextpage < 0) {
		lilist[0].className ="disabled";
		return;
	}

	var totalpage = Math.ceil(g_data.length/line_each_page);
	if (nextpage == 0 || nextpage == 1) {
		nextpage = 1;
		lilist[0].className ="disabled";
	}

	// console.log(nextpage);

	alist[0].text = nextpage.toString() + '/' + totalpage.toString();
	// console.log(alist[0].text);
	// console.log(alist[0]);
	
	if (document.getElementById("showtablenode").style.display == "block") {
		showdata(g_data, "showtablenode", (nextpage - 1).toString());
	}

	if (document.getElementById("showtableup").style.display == "block") {
		showdata(g_data, "showtableup", (nextpage - 1).toString());
	}
}

function nextpage(obj) {
	var lilist = obj.parentNode.parentNode.getElementsByTagName("li");
	// console.log(lilist);

	var alist = lilist[1].childNodes;
	var value = alist[0].text;
	var nextpagestart = parseInt(value) * line_each_page;
	if (parseInt(value) >= g_data.length 
		|| nextpagestart >= g_data.length) {
		return;
	}

	// console.log(value);
	//取整
	var nextpage = parseInt(value) + 1;
	var totalpage = Math.ceil(g_data.length/line_each_page);
	// console.log(nextpage);
	// console.log(totalpage);
	if (nextpage == totalpage) {
		lilist[2].className ="disabled";
	}

	if (nextpage > totalpage) {
		return;
	}
	
	// console.log(g_data.length);
	alist[0].text = nextpage.toString() + '/' + totalpage.toString();
	lilist[0].className ="";


	if (document.getElementById("showtablenode").style.display == "block") {
		showdata(g_data, "showtablenode", nextpagestart.toString());
	}

	if (document.getElementById("showtableup").style.display == "block") {
		showdata(g_data, "showtableup", nextpagestart.toString());
	}
}

function showdata(data, name, value) {	
	var table = [];
	if (name =="showtableup")
	{
		var tr = '<tr>'
			+ '<td><input class="style1" value="nodenumber" readonly="true" /></td>' 
			+ '<td><input class="style1" value="act" readonly="true" /></td>'
			+ '<td><input class="style1" value="dispatchstatus" readonly="true" /></td>' 
			+ '<td><input class="style3" value="ip" readonly="true" /></td>' 
			+ '<td><input class="style3" value="explain" readonly="true" /></td>' 
			+ '<td>操作</td>'
			+ '</tr>';

			var isNum = isNaN(parseInt(value));
			if (!isNum) {
				var begin = parseInt(value);
				var end = parseInt(value);
				for (var i = begin; i <= end + line_each_page - 1; i++) {
					if ( i >= data.length) {
						break;
					}
					table.push(tr.replace(/nodenumber/g, 
						data[i]["nodenumber"]).replace(/act/g, 
						data[i]["act"]).replace(/dispatchstatus/g,
						data[i]["dispatchstatus"]).replace(/explain/g,
						data[i]["explain"]).replace(/ip/g, 
						data[i]["ip"]).replace(/操作/g, totalop));
				}
			}
		
		// console.log(table);
		$('#tableup > tbody').html(table);

		document.getElementById("showtablenode").style.display = "none";
		document.getElementById("showtableup").style.display = "block";
		// var up = document.getElementById("tableup");
		// up.setAttribute("overflow", "auto");
		// console.log(up.style);
		// up.style.overflow = "auto";
	} else if (name =="showtablenode") {
		var tr = '<tr>'
			+ '<td><input class="style1" value="nodenumber" readonly="true" /></td>' 
			+ '<td><input class="style1" value="cdnnodeid" readonly="true" /></td>'
			// + '<td><input class="style1" value="hdfsname" readonly="true" /></td>'
			// + '<td><input class="style1" value="act" readonly="true" /></td>'
			// + '<td><input class="style1" value="dispatchstatus" readonly="true" /></td>' 
			+ '<td><input class="style1" value="type" readonly="true" /></td>'
			+ '<td><input class="style3" value="ip" readonly="true" /></td>' 
			+ '<td><input class="style3" value="nodename" readonly="true" /></td>' 
			+ '<td><input class="style3" value="explain" readonly="true" /></td>'
			+ '<td>操作</td>'
			+ '</tr>';

		var isNum = isNaN(parseInt(value));
		if (!isNum) {
			var begin = parseInt(value);
			var end = parseInt(value);
			for (var i = begin; i <= end + line_each_page - 1; i++) {
				if ( i >= data.length) {
					break;
				}
				table.push(tr.replace(/nodenumber/g, 
					data[i]["nodenumber"]).replace(/cdnnodeid/g,
					data[i]["cdnnodeid"]).replace(/type/g,
					data[i]["type"]).replace(/ip/g, 
					data[i]["ip"]).replace(/nodename/g,
					data[i]["nodename"]).replace(/explain/g,
					data[i]["explain"]).replace(/操作/g, totalop));
			}
		}
		
		// console.log(table);
		$('#tablenode > tbody').html(table);

		document.getElementById("showtablenode").style.display = "block";
		document.getElementById("showtableup").style.display = "none";
	}
}

function setpage(data, name) {
	var pagelist = document.getElementById("page");
	var pagelistbody = pagelist.parentNode;
	var li_list = pagelist.getElementsByTagName("li");

	if (li_list.length != 0) {
		// delete li in pagelist
		while (pagelist.firstChild) {
    			pagelist.removeChild(pagelist.firstChild);
		}
	}

	var newli = document.createElement("li");
	newli.innerHTML = '<a href="#" onclick="prepage(this)">←上一页</a>';
	newli.className ="disabled";
	pagelist.appendChild(newli);

	var newline = document.createElement("li");
	var totalpage = Math.ceil(g_data.length/line_each_page);
	newline.innerHTML = '<a>' + '1' + '/' + totalpage.toString() + '</a>';
	pagelist.appendChild(newline);

	var newli = document.createElement("li");
	newli.innerHTML = '<a href="#" onclick="nextpage(this)">下一页→</a>';
	// 一页能全部显示就不能点击下一页
	if (line_each_page >= g_data.length) {
		newli.className ="disabled";
	}
	pagelist.appendChild(newli);

	pagelistbody.appendChild(pagelist);
	showdata(data, name, "0");
}

function updateList(data, name) {
	g_data = data;
	// console.log(g_data.length);
	setpage(g_data, name); 
}

function operateUploadServer(op, e) {
	var ip = "";
	var nodenumber = "";
	var explain = "";
	var act = "";
	var dispatchstatus = "";
	var dataurl = "";
	switch (op) {
		case 0:
			dataurl = url + "/" + "opUploadServer" + "?" + "op=" + op + "&nodenumber=" 
				+ e[0] + "&act=" + e[1] + "&dispatchstatus=" + e[2] + "&ip=" + e[3] + "&explain=" + e[4];
			break;
		case 1:
			dataurl = url + "/" + "opUploadServer" + "?" + "op=" + op + "&nodenumber=" + e + "&ip=" + "0";
			break;
		case 2:
			dataurl = url + "/" + "opUploadServer" + "?" + "op=" + op + "&nodenumber=" 
				+ e[0] + "&act=" + e[1] + "&dispatchstatus=" + e[2] + "&ip=" + e[3] + "&explain=" + e[4] 
				+ "&nodenumberkey=" + e[5];
			break;
		case 3:
			var NodeID = document.getElementById('searchStringID').value; 
			if (NodeID.length == 0) {
				alert("请输入nodenumber!");
				return;
			}

			var isNum = isNaN(parseInt(NodeID));
			if (isNum) {
				alert("请输入合法nodenumber!");
				return;
			}

			var isPositive = isPositiveNum(NodeID)
			if (!isPositive) {
				alert("请输入正整数nodenumber!");
				return;
			}

			document.getElementById('searchStringID').value = "";
			dataurl = url + "/" + "opUploadServer" + "?" + "op=" + op + "&nodenumber=" + NodeID;
			break;
		case 4:
			dataurl = url + "/" + "opUploadServer" + "?" + "op=" + op + "&ip=" + "0";
			break;
		default:
			break;
	}

	// console.log(dataurl);

	$.ajax({
		url: dataurl,
		type: 'get',
		success: function (data) {
		var JSON = $.parseJSON(data);
		if (JSON["result"] == "true") {
			// alert("Success!");
			shownewtableup();
			return;
		}
		else if (JSON["result"] == "false") {
			alert("Fail!");
			shownewtableup();
			return;
		}
		updateList(JSON["result"], "showtableup");
	}
	});
}

function operateUploadNode(op, e) {
	var dataurl = "";

	switch (op) {
		case 0:
			dataurl = url + "/" + "opUploadNode" + "?" + "op=" + op
				+ "&nodenumber=" + e[0] + "&cdnnodeid=" + e[1] + "&type=" + e[2] 
				+ "&ip=" + e[3] + "&nodename=" + e[4] + "&explain=" + e[5];
			break;
		case 1:
			dataurl = url + "/" + "opUploadNode" + "?" + "op=" + op + "&nodenumber=" + e;
			break;
		case 2:
			dataurl = url + "/" + "opUploadNode" + "?" + "op=" + op
				+ "&nodenumber=" + e[0] + "&cdnnodeid=" + e[1] + "&type=" + e[2]
				+ "&ip=" + e[3] + "&nodename=" + e[4] + "&explain=" + e[5] + "&nodenumberkey=" + e[6];
			break;
		case 3:
			var NodeID = document.getElementById('searchStringID').value; 
			if (NodeID.length == 0) {
				alert("请输入nodenumber!");
				return;
			}

			var isNum = isNaN(parseInt(NodeID));
			if (isNum) {
				alert("请输入合法nodenumber!");
				return;
			}

			var isPositive = isPositiveNum(NodeID)
			if (!isPositive) {
				alert("请输入正整数nodenumber!");
				return;
			}

			document.getElementById('searchStringID').value = "";
			dataurl = url + "/" + "opUploadNode" + "?" + "op=" + op + "&nodenumber=" + NodeID;
			break;
			
		case 4:
			dataurl = url + "/" + "opUploadNode" + "?" + "op=" + op + "&nodenumber=" + "0";
			break;
		default:
			break;
	}

	// console.log(dataurl);

	$.ajax({
		url: dataurl,
		type: 'get',
		success: function (data) {
		var JSON = $.parseJSON(data);
		if (JSON["result"] == "true") {
			// alert("Success!");
			shownewtablenode();
			return;
		}
		// console.log(JSON);
		updateList(JSON["result"], "showtablenode");
	}
	});
}

function showNodeForm(e) {
	var op = "";
	var id = "";
	
	switch (e.id) {
		case "selectA":
			//查询所有上传机信息
			document.getElementById("showsearch").style.display = "block";
			document.getElementById("page").style.display = "block";
			document.getElementById("showstatic").style.display = "none";

			id = "A";
			e.data = id;
			operateUploadServer(4, e);
			break;
		case "selectRA":
			// 查询所有上传节点信息
			document.getElementById("showsearch").style.display = "block";
			document.getElementById("page").style.display = "block";
			document.getElementById("showstatic").style.display = "none";
			id = "RA";
			e.data = id;
			operateUploadNode(4, e);
			break;
		case "static":
			// 图表统计
			document.getElementById("showstatic").style.display = "block";
			document.getElementById("showsearch").style.display = "none";
			document.getElementById("showtableup").style.display = "none";
			document.getElementById("showtablenode").style.display = "none";
			document.getElementById("page").style.display = "none";
			break;
		default:
			break;
	}
}

function isPositiveNum(s){//是否为正整数  
    var re = /^[0-9]*[1-9][0-9]*$/;  
    return re.test(s)  
} 

// bind enter event
// document.onkeydown = function(e){
// 	if(!e){
// 		e = window.event;
// 	}

// 	if((e.keyCode || e.which) == 13){
// 		if (document.getElementById("showtablenode").style.display == "block") {
// 			operateUploadNode(3, e);
// 		}

// 		if (document.getElementById("showtableup").style.display == "block") {
// 			operateUploadServer(3, e);
// 		}
// 	}
// }

function search(e) {
	if (document.getElementById("showtablenode").style.display == "block") {
		operateUploadNode(3, e);
	} else if (document.getElementById("showtableup").style.display == "block") {
		operateUploadServer(3, e);
	}
}

function echarts() {
	self.location="./static/echartTest.html";
}

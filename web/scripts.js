(function () {
    var input = document.getElementById('query');
    var lbutton = document.getElementById('left');
    var rbutton = document.getElementById('right');
    var list = document.getElementById('result-list');
    var search_id = "";

    function add_res(number, data) {
        console.log(data);
        var url = data.url;
        var title = data.title;
        var snippet = data.snippet;

        $("#result-list").append(
                "<li class ='elem' ><a class='result' href='" + url + "'> " + title + "</a><small><br>" + snippet + "</small></li>");
    }

    function get_results(search_id, pg){
        $.ajax({
            url: "http://192.168.77.40:5000/get_results/"+search_id + '/' + pg, 
            type: 'GET',
            crossDomain: true,
            dataType: 'json',
            //contentType: "application/json",
            success: function(data) {
                var docs = data.docs;
                while (list.hasChildNodes()) list.removeChild(list.childNodes[0]);
                $.each(docs, add_res);
                
                input.dataset.mxpage = data.page_number;
            },
            error: function() { alert('Failed to load results!'); },
        });

    }

    function search(query){
        var json = {
            query: query,
        }
        input.dataset.page = 0;
        console.log(JSON.stringify(json));

        $.ajax({
            url: "http://192.168.77.40:5000/search/", 
            type: 'POST',
            crossDomain: true,
            contentType: "application/json",
            success: function(data) {
                get_results(data, 0);
                search_id = data;                    
            },
            error: function() { alert('Failed to send search request!'); },
            data: JSON.stringify(json)
        });

    }

    function update(){
        if (input.dataset.query == "") {return}
        search(input.dataset.query);
    }

	lbutton.addEventListener('click', function(e) {
        var n = parseInt(input.dataset.page);
		if (n < 1) {
            return;
        }
        input.dataset.page = n - 1;
        get_results(search_id, input.dataset.page);
	});

	rbutton.addEventListener('click', function(e) {
        var n = parseInt(input.dataset.page);
        var m = parseInt(input.dataset.mxpage);
		if (n >= m-1) {
            return;
        }
        input.dataset.page = n + 1;
        get_results(search_id, input.dataset.page);
	});

	input.addEventListener('keydown', function(e) {
        if (e.keyCode != 13) {return}
        input.dataset.query = input.value;
        update();
	});

    update();

})();

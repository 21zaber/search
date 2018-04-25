(function () {
    var input = document.getElementById('query');
    var lbutton = document.getElementById('left');
    var rbutton = document.getElementById('right');
    var list = document.getElementById('result-list');


    function add_res(number, data) {
        console.log(data);
        var url = data.url;
        var title = data.title;

        $("#result-list").append(
                "<li class ='elem' ><a class='result' href='" + url + "'> " + title + "</li>");
    }

    function search(query, pg){
        $.ajax({
            url: "http://192.168.77.40:5000/search/" + query + "/" + pg, 
            type: 'GET',
            crossDomain: true,
            dataType: 'json',
            contentType: "application/json",
            success: function(data) { 
                var docs = data.docs;
                $.each(docs, add_res);

                input.dataset.mxpage = data.page_number;
            },
            error: function() { alert('Failed to load results!'); },
        });
    }

    function update(){
        if (input.dataset.query == "") {return}
        while (list.hasChildNodes()) list.removeChild(list.childNodes[0]);
        search(input.dataset.query, input.dataset.page);
    }

	lbutton.addEventListener('click', function(e) {
        var n = parseInt(input.dataset.page);
		if (n < 1) {
            return;
        }
        input.dataset.page = n - 1;
        update();
	});

	rbutton.addEventListener('click', function(e) {
        var n = parseInt(input.dataset.page);
        var m = parseInt(input.dataset.mxpage);
		if (n >= m-1) {
            return;
        }
        input.dataset.page = n + 1;
        update();
	});

	input.addEventListener('keydown', function(e) {
        if (e.keyCode != 13) {return}
        input.dataset.query = input.value;
        update();
	});

    update();

})();

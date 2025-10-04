jQuery(function($) {
    $('#ollama-submit').on('click', function() {
        const question = $('#ollama-question').val();
        $('#ollama-answer').html('Thinking...');

        $.post(OllamaChat.ajax_url, {
            action: 'ollama_submit',
            question: question,
            _ajax_nonce: OllamaChat.nonce
        }, function(res) {
            if (!res.success) {
                $('#ollama-answer').html('Error: ' + res.data);
                return;
            }
            const qid = res.data.id;

            // Poll for answer
            const interval = setInterval(function() {
                $.post(OllamaChat.ajax_url, { action: 'ollama_get_answer', id: qid }, function(ans) {
                    if (ans.success && ans.data.status === 'done') {
                        clearInterval(interval);
                        $('#ollama-answer').html(ans.data.answer);
                    }
                });
            }, 3000);
        });
    });
});

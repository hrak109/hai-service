<?php
/**
 * Plugin Name: Ollama Chat
 * Description: Public Q&A chat powered by Ollama + worker.
 * Version: 2.0
 * Author: Hee Bae
 */

if (!defined('ABSPATH')) exit;

global $wpdb;
$table = $wpdb->prefix . 'ollama_queue';

// Create queue table on activation
register_activation_hook(__FILE__, function() use ($table, $wpdb) {
    $charset_collate = $wpdb->get_charset_collate();
    $sql = "CREATE TABLE $table (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        question TEXT NOT NULL,
        answer LONGTEXT,
        status VARCHAR(20) DEFAULT 'pending',
        created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) $charset_collate;";
    require_once(ABSPATH . 'wp-admin/includes/upgrade.php');
    dbDelta($sql);
});

// Enqueue scripts
add_action('wp_enqueue_scripts', function() {
    wp_enqueue_script('ollama-chat', plugin_dir_url(__FILE__) . 'ollama-chat.js', ['jquery'], '1.0', true);
    wp_localize_script('ollama-chat', 'OllamaChat', [
        'ajax_url' => admin_url('admin-ajax.php'),
        'nonce'    => wp_create_nonce('ollama_chat_nonce'),
    ]);
});

// Shortcode for chat form
add_shortcode('ollama_chat', function() {
    ob_start(); ?>
    <div id="ollama-chat-box">
        <textarea id="ollama-question" placeholder="Ask me anything..."></textarea>
        <button id="ollama-submit">Send</button>
        <div id="ollama-answer"></div>
    </div>
    <?php return ob_get_clean();
});

// Handle new question (PUBLIC — requires nonce)
add_action('wp_ajax_nopriv_ollama_submit', 'ollama_submit');
add_action('wp_ajax_ollama_submit', 'ollama_submit');
function ollama_submit() {
    check_ajax_referer('ollama_chat_nonce');
    global $wpdb, $table;

    $question = sanitize_textarea_field($_POST['question'] ?? '');
    if (!$question) wp_send_json_error('Missing question');

    $wpdb->insert($table, ['question' => $question, 'status' => 'pending']);
    wp_send_json_success(['id' => $wpdb->insert_id]);
}

// Handle fetching answer (PUBLIC — no nonce needed)
add_action('wp_ajax_nopriv_ollama_get_answer', 'ollama_get_answer');
add_action('wp_ajax_ollama_get_answer', 'ollama_get_answer');
function ollama_get_answer() {
    global $wpdb, $table;
    $id = intval($_POST['id'] ?? 0);
    $row = $wpdb->get_row($wpdb->prepare("SELECT * FROM $table WHERE id=%d", $id));
    if (!$row) wp_send_json_error('Not found');
    wp_send_json_success(['answer' => $row->answer, 'status' => $row->status]);
}

// Worker updates answer (PRIVATE — requires secret key)
add_action('wp_ajax_nopriv_ollama_update_answer', 'ollama_update_answer');
add_action('wp_ajax_ollama_update_answer', 'ollama_update_answer');
function ollama_update_answer() {
    $secret = $_POST['secret'] ?? '';
    if ($secret !== 'a8Fj39sdlfjKJ!93jf02') {
        wp_send_json_error('Unauthorized', 403);
    }

    global $wpdb, $table;
    $id = intval($_POST['id'] ?? 0);
    $answer = sanitize_textarea_field($_POST['answer'] ?? '');
    if (!$id) wp_send_json_error('Missing ID');

    $wpdb->update($table, ['answer' => $answer, 'status' => 'done'], ['id' => $id]);
    wp_send_json_success();
}

add_action('rest_api_init', function() {
    register_rest_route('ollama/v1', '/next', [
        'methods' => 'GET',
        'callback' => function() {
            global $wpdb, $table;
            $row = $wpdb->get_row("SELECT * FROM $table WHERE status='pending' ORDER BY created ASC LIMIT 1");
            return $row ?: [];
        },
    ]);
});

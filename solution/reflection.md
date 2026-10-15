# Tự đánh giá — Data Siege

**Cách tiếp cận.** Tôi đăng ký đủ năm handler và dùng đúng các công cụ trong API.
Mỗi sự kiện chỉ cần một lần đo; kết quả được lưu theo công cụ và tham số để
không trả phí hai lần cho cùng phép đo. Các quyết định dựa trên số đo thực tế,
ngưỡng baseline và lịch sử trong `ctx.state`, không dựa vào ID hay đáp án của
một phase. Lỗi trả về từ công cụ được xử lý riêng, không coi là lỗi dữ liệu.

**Loại lỗi khó nhất và nguyên nhân.** Những sai lệch nhỏ còn nằm trong khoảng
baseline khó phân biệt với dao động tự nhiên. Với batch, tôi kết hợp độ lệch
của số dòng và giá trị đơn hàng để phát hiện hai tín hiệu vừa phải xuất hiện
đồng thời. Với các chỉ số một phía, sau ít nhất 16 quan sát tôi dùng median và
MAD, giữ ngưỡng baseline làm giới hạn trên và đặt sàn cho độ phân tán để tránh
học ngưỡng quá hẹp. Những lỗi xảy ra sớm vẫn có thể bị bỏ sót. Lineage cũng khó
khi payload không khai báo topology mong đợi: phải chờ lịch sử đủ ổn định mới
phân biệt được cạnh bị mất với một topology hợp lệ khác. Topology được so sánh
ngay khi có thông tin kỳ vọng; thứ tự cạnh không ảnh hưởng kết quả.

**Chi phí và độ bao phủ.** Tôi chọn kiểm tra mọi sự kiện thay vì dừng kiểm tra
ở cuối luồng khi ngân sách thấp. Public đạt 42,56 điểm, TPR 89,74%, FPR 1,65%,
với chi phí 240/220 credits. Đây là sự đánh đổi có chủ ý: chấp nhận phần phạt
chi phí nhỏ để giữ độ bao phủ. Private đạt 28,70 điểm, TPR 57,41%, FPR 0% và
không vượt ngân sách. Kết quả private cho thấy giải pháp còn thận trọng với
các lỗi tinh vi; không có nhãn chi tiết để kết luận chính xác lỗi nào bị bỏ sót.

**Nếu có thêm một lượt phát triển.** Tôi sẽ dùng một tập practice độc lập để
hiệu chỉnh ngưỡng tổng hợp, đánh giá thời gian khởi động của bộ học topology
và ước lượng độ bất định của các chỉ số drift. Tôi cũng sẽ thử chính sách phân
bổ credits theo lợi ích phát hiện của từng phép đo, đo cả mức giảm TPR trước
khi áp dụng. Không chỉnh ngưỡng theo điểm private hoặc mã sự kiện. Trong phiên
này practice không tạo được báo cáo; sau đó harness báo phase chưa được mở khóa.
Selfcheck và tám kiểm thử giả lập đã đạt; public được dùng để kiểm tra tích hợp.

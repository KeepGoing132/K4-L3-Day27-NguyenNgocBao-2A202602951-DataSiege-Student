# Tự đánh giá — Data Siege

**Cách tiếp cận.** Tôi triển khai đủ năm handler, mỗi sự kiện gọi một công cụ
đúng loại và lưu kết quả theo công cụ/tham số trong `ctx.state` để tránh trả phí
trùng lặp. Quyết định dựa vào số đo, baseline, hợp đồng của pipeline và lịch sử
quan sát; không dựa vào mã sự kiện hoặc đáp án cố định. Lỗi công cụ được trả về
với độ tin cậy bằng 0, không đánh đồng với dữ liệu sạch đã kiểm tra thành công.

**Loại lỗi khó nhất.** Sai lệch nhỏ nằm gần phân phối sạch dễ bị bỏ sót nếu chỉ
sử dụng ngưỡng đơn lẻ. Tôi kết hợp độ lệch số dòng và giá trị đơn hàng, đồng thời
sử dụng median/MAD với thời gian khởi động và sàn độ phân tán cho chỉ số một phía.
Feature skew dùng ngưỡng 0,65 theo bài học, vì dữ liệu sạch có thể vượt baseline
lịch sử 0,4095. Với embedding, giới hạn centroid là 0,04 và tuổi tài liệu là 45
ngày, dựa trên khoảng phân tách được tài liệu bài học cung cấp.

Lineage cần kiến thức cấu trúc ngay từ sự kiện đầu tiên: model đơn hàng bắt
buộc có `raw.orders` và `raw.customers`; output không có downstream là lỗi
orphan, trừ khi payload khai báo rõ đó là sink hợp lệ. Cách học topology từ
lịch sử đơn thuần từng bỏ sót một lỗi missing-upstream ở practice. Tôi sửa
bằng quy tắc phụ thuộc của pipeline, không thêm ngoại lệ theo ID sự kiện.

**Chi phí và kết quả.** Tôi kiểm tra mọi sự kiện, chấp nhận phần phạt nhỏ ở
public để giữ độ bao phủ. Practice đạt 50,00 điểm: TP=33, FP=0, FN=0, TN=87,
chi phí 180/220 credits. Public đạt 46,90 điểm: TPR 97,44%, FPR 0%, cả bốn
trụ cột đều high, chi phí 240/220 credits. Private đạt 32,41 điểm: TPR 64,81%,
FPR 0%, không vượt ngân sách. Private vẫn còn lỗi bị bỏ sót; báo cáo tổng hợp
không đủ để xác định chúng thuộc loại nào. Các báo cáo được harness tạo và ký,
không sửa thủ công. Selfcheck và 12 kiểm thử giả lập đều đạt.

**Nếu có thêm thời gian.** Tôi sẽ đánh giá trên nhiều luồng practice độc lập,
hiệu chỉnh ngưỡng tổng hợp theo cả bốn chiều chất lượng dữ liệu và ước lượng
độ bất định khi lịch sử còn ngắn. Tôi cũng muốn so sánh thống kê theo từng
model/feature view và phát hiện thay đổi theo thời gian. Mọi điều chỉnh cần
được kiểm tra về FPR và chi phí, tránh hạ ngưỡng chỉ để tăng TPR hoặc dùng điểm
private làm tín hiệu dò ngược đáp án.

package GUI;
import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import javax.swing.WindowConstants;
import javax.swing.SwingUtilities;

import injector.*;
import fetchList.*;
import fetch.*;
import view.showDb;


/**
* This code was edited or generated using CloudGarden's Jigloo
* SWT/Swing GUI Builder, which is free for non-commercial
* use. If Jigloo is being used commercially (ie, by a corporation,
* company or business for any purpose whatever) then you
* should purchase a license for each developer using Jigloo.
* Please visit www.cloudgarden.com for details.
* Use of Jigloo implies acceptance of these licensing terms.
* A COMMERCIAL LICENSE HAS NOT BEEN PURCHASED FOR
* THIS MACHINE, SO JIGLOO OR THIS CODE CANNOT BE USED
* LEGALLY FOR ANY CORPORATE OR COMMERCIAL PURPOSE.
*/
public class gui extends javax.swing.JFrame {

	{
		//Set Look & Feel
		try {
			javax.swing.UIManager.setLookAndFeel("com.sun.java.swing.plaf.gtk.GTKLookAndFeel");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JPanel jPanel1;
	private JButton fetch;
	private JScrollPane jScrollPane1;
	private JTextArea text;
	private JLabel jLabel3;
	private JLabel jLabel2;
	private JLabel jLabel1;
	private JTextField interval;
	private JTextField depth;
	private JButton view;
	private JButton generate;
	private JButton inject;

	/**
	* Auto-generated main method to display this JFrame
	*/
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				gui inst = new gui();
				inst.setLocationRelativeTo(null);
				inst.setVisible(true);
			}
		});
	}
	
	public gui() {
		super();
		initGUI();
	}
	
	private void initGUI() {
		try {
			this.setSize(700, 450);
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			{
				jPanel1 = new JPanel();
				getContentPane().add(jPanel1, BorderLayout.CENTER);
				jPanel1.setPreferredSize(new java.awt.Dimension(444, 292));
				jPanel1.setLayout(null);
				{
					inject = new JButton();
					jPanel1.add(inject);
					inject.setText("inject");
					inject.setBounds(19, 221, 78, 27);
					inject.setSize(80, 27);
					injectListener il=new injectListener();
					inject.addActionListener(il);
				}
				{
					generate = new JButton();
					jPanel1.add(generate);
					generate.setText("generate");
					generate.setBounds(106, 221, 80, 27);
					generatListener gl=new generatListener();
					generate.addActionListener(gl);
				}
				{
					fetch = new JButton();
					jPanel1.add(fetch);
					fetch.setText("fetch");
					fetch.setBounds(191, 221, 80, 27);
					fetchListener fl=new fetchListener();
					fetch.addActionListener(fl);
				}
				{
					view = new JButton();
					jPanel1.add(view);
					view.setText("view");
					view.setBounds(282, 221, 49, 27);
					view.setSize(80, 27);
					viewListener vl=new viewListener();
					view.addActionListener(vl);
				}
				{
					depth = new JTextField();
					jPanel1.add(depth);
					depth.setBounds(83, 18, 66, 23);
				}
				{
					interval = new JTextField();
					jPanel1.add(interval);
					interval.setBounds(236, 18, 82, 23);
				}
				{
					jLabel1 = new JLabel();
					jPanel1.add(jLabel1);
					jLabel1.setText("depth");
					jLabel1.setBounds(35, 19, 46, 22);
				}
				{
					jLabel2 = new JLabel();
					jPanel1.add(jLabel2);
					jLabel2.setText("interval");
					jLabel2.setBounds(177, 17, 57, 25);
				}
				{
					jLabel3 = new JLabel();
					jPanel1.add(jLabel3);
					jLabel3.setText("s");
					jLabel3.setBounds(324, 18, 16, 23);
				}
				{
					jScrollPane1 = new JScrollPane();
					jPanel1.add(jScrollPane1);
					jScrollPane1.setBounds(19, 64, 343, 135);
					{
						text = new JTextArea();
						jScrollPane1.setViewportView(text);
					}
				}
			}
			pack();
			setSize(400, 300);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private class injectListener implements ActionListener {
//监听程序界面的inject按钮的类
		@Override
		public void actionPerformed(ActionEvent arg0) {
			// 按下按钮触发的方法
			try {
				FileWriter fw=new FileWriter(System.getProperty("user.dir")+"/crawl/input/url.txt");
				//fw缓冲区用来向url.txt写入字符流
				BufferedWriter bw=new BufferedWriter(fw);
				//bw是缓冲字符输出流，用来向上一步构建的写入文件的缓冲区写入字符
				String t=text.getText();
				//从界面的文本框中获取输入的url文本
				bw.write(t);
				bw.flush();
				bw.close();
				fw.close();
				injector i=new injector();
				//新建能读取文本文件中url，并将url做为入口地址注入到数据库的对象
				long in;
				//获取界面输入的interval
				if(interval.getText().equals(""))
					in=3600*24*1000;
				else
					in=Long.parseLong(interval.getText())*1000;
				i.inject(in);
				//将interval作为参数传入inject方法中
				interval.setText("");
				text.setText("");
				//text.setText("inject success \n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private class generatListener implements ActionListener {
//监听界面上generat这个按钮的触发状态
		@Override
		//按钮触发后调用的函数
		public void actionPerformed(ActionEvent e) {
			// genetator 主要用来生成待下载的URL列表，提取未下载的URL，生成fetch_list为下载做准备
			generator gen=new generator();
			try {
				gen.generate();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	private class fetchListener implements ActionListener {
//监听界面上fetch按钮的触发状态
		@Override
		public void actionPerformed(ActionEvent e) {
			// TODO Auto-generated method stub
			int d;
			if(depth.getText().equals(""))
				d=0;
			else
				d=Integer.parseInt(depth.getText());
			//以上部分是用界面获取文本框中抓取深度的代码
			fetcher f=new fetcher();
			//新建用来抓取的对象
			try {
				f.fetch(d);
				depth.setText("");
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	private class viewListener implements ActionListener {

		@Override
		public void actionPerformed(ActionEvent e) {
			// TODO Auto-generated method stub
			String path=System.getProperty("user.dir")+"/crawl/dbviewer/part-00000";
			showDb show=new showDb();
			try {
				show.show();
				FileReader fd=new FileReader(path);
				BufferedReader br=new BufferedReader(fd);
				String t="";
				String result="";
				while((t=br.readLine())!=null) {
					result=result+t+"\n";
				}
				text.setText(result);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
}
